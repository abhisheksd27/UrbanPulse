"""
UrbanPulse - PySpark Job: Bronze to Silver (Staging)
====================================================
Transforms raw ingested taxi trip data into cleaned, typed, deduplicated,
and spatially-enriched Silver tables.

Execution:
  - AWS EMR:   spark-submit --master yarn spark/jobs/bronze_to_silver.py --year 2024 --month 1
  - Local Dev: python -m spark.jobs.bronze_to_silver --year 2024 --month 1 --local

Transformations Applied:
  1. Schema Enforcement & Timestamp Parsing
  2. Deduplication on trip_id
  3. Validation Filters: fare >= 0, distance > 0, 0.5 <= duration_minutes <= 720
  4. Geospatial Enrichment: Nearest neighborhood centroid lookup for pickup & dropoff
  5. Derived Metrics: duration_minutes, distance_km, total_amount, is_airport, is_downtown
  6. Partitioned Write to staging/taxi/year=YYYY/month=MM/
"""

from __future__ import annotations

import argparse
import datetime
import logging
import math
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from ingestion.config.paths import (
    format_year_month,
    get_raw_geo_key,
    get_raw_taxi_key,
    get_staging_taxi_prefix,
    to_local_path,
)
from ingestion.config.s3_storage import StorageSink
from ingestion.config.settings import settings
from spark.jobs.schema_definitions import PYSPARK_AVAILABLE, SILVER_TAXI_COLUMNS
from spark.utils.s3_utils import load_dataset_records, resolve_spark_path, write_dataset_records
from spark.utils.spark_session import get_spark_session

logger = logging.getLogger("urbanpulse.bronze_to_silver")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [BronzeToSilver] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# Default fallback SF Neighborhood centroids (used if geo reference file is loading)
SF_NEIGHBORHOOD_CENTROIDS = [
    {"name": "Financial District / South Beach", "district": "District 6", "lat": 37.7946, "lng": -122.3999, "downtown": True, "airport": False},
    {"name": "Mission", "district": "District 9", "lat": 37.7599, "lng": -122.4148, "downtown": False, "airport": False},
    {"name": "South of Market (SoMa)", "district": "District 6", "lat": 37.7785, "lng": -122.4056, "downtown": True, "airport": False},
    {"name": "Marina", "district": "District 2", "lat": 37.8037, "lng": -122.4368, "downtown": False, "airport": False},
    {"name": "Castro / Upper Market", "district": "District 8", "lat": 37.7609, "lng": -122.4350, "downtown": False, "airport": False},
    {"name": "Nob Hill", "district": "District 3", "lat": 37.7932, "lng": -122.4145, "downtown": True, "airport": False},
    {"name": "Pacific Heights", "district": "District 2", "lat": 37.7925, "lng": -122.4352, "downtown": False, "airport": False},
    {"name": "Tenderloin", "district": "District 6", "lat": 37.7847, "lng": -122.4145, "downtown": True, "airport": False},
    {"name": "Chinatown", "district": "District 3", "lat": 37.7941, "lng": -122.4078, "downtown": True, "airport": False},
    {"name": "North Beach", "district": "District 3", "lat": 37.8005, "lng": -122.4102, "downtown": True, "airport": False},
    {"name": "Haight Ashbury", "district": "District 5", "lat": 37.7692, "lng": -122.4481, "downtown": False, "airport": False},
    {"name": "Inner Sunset", "district": "District 7", "lat": 37.7628, "lng": -122.4659, "downtown": False, "airport": False},
    {"name": "Outer Sunset", "district": "District 4", "lat": 37.7540, "lng": -122.4941, "downtown": False, "airport": False},
    {"name": "Inner Richmond", "district": "District 1", "lat": 37.7786, "lng": -122.4646, "downtown": False, "airport": False},
    {"name": "Outer Richmond", "district": "District 1", "lat": 37.7770, "lng": -122.4975, "downtown": False, "airport": False},
    {"name": "Presidio", "district": "District 2", "lat": 37.7989, "lng": -122.4662, "downtown": False, "airport": False},
    {"name": "Bayview Hunters Point", "district": "District 10", "lat": 37.7344, "lng": -122.3842, "downtown": False, "airport": False},
    {"name": "Potrero Hill", "district": "District 10", "lat": 37.7571, "lng": -122.3999, "downtown": False, "airport": False},
    {"name": "Bernal Heights", "district": "District 9", "lat": 37.7416, "lng": -122.4150, "downtown": False, "airport": False},
    {"name": "San Francisco International Airport (SFO)", "district": "Airport", "lat": 37.6213, "lng": -122.3790, "downtown": False, "airport": True},
]


def find_nearest_neighborhood(lat: float, lng: float) -> Tuple[str, str, bool, bool]:
    """
    Finds nearest SF neighborhood by Euclidean coordinate distance.
    Returns (neighborhood_name, district, is_downtown, is_airport).
    """
    if pd.isna(lat) or pd.isna(lng) or lat == 0 or lng == 0:
        return "Unknown", "Unknown", False, False

    best_match = SF_NEIGHBORHOOD_CENTROIDS[0]
    best_dist = float("inf")

    for n in SF_NEIGHBORHOOD_CENTROIDS:
        # Scale longitude by cos(mean latitude) to account for earth curvature
        dlat = lat - n["lat"]
        dlng = (lng - n["lng"]) * 0.79  # cos(37.7 degrees) ~ 0.79
        dist = dlat * dlat + dlng * dlng
        if dist < best_dist:
            best_dist = dist
            best_match = n

    return (
        best_match["name"],
        best_match["district"],
        best_match["downtown"],
        best_match["airport"],
    )


def clean_and_transform_taxi_dataframe(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Core transformation engine: applies cleaning, validation,
    spatial enrichment, and derived metric calculation.
    """
    if df_raw.empty:
        return pd.DataFrame(columns=list(SILVER_TAXI_COLUMNS.keys()))

    initial_count = len(df_raw)
    logger.info("Applying Silver transformations to %d raw records...", initial_count)

    # 1. Deduplication on trip_id
    df = df_raw.drop_duplicates(subset=["trip_id"]).copy()
    logger.info("Deduplication: removed %d duplicate rows (remaining: %d)", initial_count - len(df), len(df))

    # 2. Parse pickup and dropoff timestamps
    df["pickup_at"] = pd.to_datetime(df["pickup_datetime"], errors="coerce", utc=True)
    df["dropoff_at"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce", utc=True)

    # Filter out null timestamps
    df = df[df["pickup_at"].notna() & df["dropoff_at"].notna()].copy()

    # 3. Numeric conversions
    df["fare_amount"] = pd.to_numeric(df["fare"], errors="coerce").fillna(0.0)
    df["tip_amount"] = pd.to_numeric(df["tips"], errors="coerce").fillna(0.0)
    df["total_amount"] = (df["fare_amount"] + df["tip_amount"]).round(2)
    df["distance_miles"] = pd.to_numeric(df["distance"], errors="coerce").fillna(0.0)
    df["distance_km"] = (df["distance_miles"] * 1.60934).round(3)

    df["pickup_lat"] = pd.to_numeric(df["pickup_latitude"], errors="coerce")
    df["pickup_lng"] = pd.to_numeric(df["pickup_longitude"], errors="coerce")
    df["dropoff_lat"] = pd.to_numeric(df["dropoff_latitude"], errors="coerce")
    df["dropoff_lng"] = pd.to_numeric(df["dropoff_longitude"], errors="coerce")

    # 4. Compute trip duration in minutes
    duration_secs = (df["dropoff_at"] - df["pickup_at"]).dt.total_seconds()
    df["duration_minutes"] = (duration_secs / 60.0).round(2)

    # 5. Data Quality Filters
    # Valid trip: fare >= 0, distance > 0, duration between 30s and 12 hours
    valid_mask = (
        (df["fare_amount"] >= 0.0)
        & (df["distance_miles"] > 0.0)
        & (df["duration_minutes"] >= 0.5)
        & (df["duration_minutes"] <= 720.0)
    )
    df_clean = df[valid_mask].copy()
    logger.info(
        "Data quality filters: %d valid records retained (%d invalid dropped)",
        len(df_clean),
        len(df) - len(df_clean),
    )

    # 6. Date partition attributes
    df_clean["pickup_date"] = df_clean["pickup_at"].dt.strftime("%Y-%m-%d")
    df_clean["pickup_hour"] = df_clean["pickup_at"].dt.hour

    # 7. Spatial Enrichment
    p_names, p_districts, p_dtown, p_air = [], [], [], []
    d_names, d_districts, d_dtown, d_air = [], [], [], []

    for _, row in df_clean.iterrows():
        p_res = find_nearest_neighborhood(row["pickup_lat"], row["pickup_lng"])
        d_res = find_nearest_neighborhood(row["dropoff_lat"], row["dropoff_lng"])

        p_names.append(p_res[0])
        p_districts.append(p_res[1])
        p_dtown.append(p_res[2])
        p_air.append(p_res[3])

        d_names.append(d_res[0])
        d_districts.append(d_res[1])
        d_dtown.append(d_res[2])
        d_air.append(d_res[3])

    df_clean["pickup_neighborhood"] = p_names
    df_clean["pickup_district"] = p_districts
    df_clean["dropoff_neighborhood"] = d_names
    df_clean["dropoff_district"] = d_districts
    df_clean["is_airport_trip"] = [a1 or a2 for a1, a2 in zip(p_air, d_air)]
    df_clean["is_downtown_trip"] = [dt1 or dt2 for dt1, dt2 in zip(p_dtown, d_dtown)]

    df_clean["processed_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    cols_to_keep = [c for c in SILVER_TAXI_COLUMNS.keys() if c in df_clean.columns]
    return df_clean[cols_to_keep]


def run_bronze_to_silver(
    year: int,
    month: int,
    force_local: bool = False,
) -> str:
    """
    Executes the Bronze to Silver ETL job for specified year and month.
    """
    y_str, m_str = format_year_month(year, month)
    raw_s3_key = get_raw_taxi_key(year, month)
    staging_prefix = get_staging_taxi_prefix(year, month)
    output_filename = f"{staging_prefix}part-00000.parquet"

    logger.info("Executing Bronze to Silver Job for %s-%s", y_str, m_str)
    logger.info("Input Bronze Key:  %s", raw_s3_key)
    logger.info("Output Silver Key: %s", output_filename)

    sink = StorageSink(force_local=force_local)

    # Check if raw data exists
    if not sink.object_exists(raw_s3_key):
        raise FileNotFoundError(
            f"Raw Bronze data not found at {raw_s3_key}. "
            f"Run Step 2 (ingestion) first: make ingest-taxi YEAR={year} MONTH={month}"
        )

    # Attempt PySpark execution if running in cluster environment
    spark = get_spark_session(app_name=f"UrbanPulse-BronzeToSilver-{y_str}-{m_str}")
    if spark is not None and not force_local:
        try:
            logger.info("Running job via Apache Spark engine...")
            input_uri = resolve_spark_path(raw_s3_key, force_local=False)
            output_uri = resolve_spark_path(staging_prefix, force_local=False)

            df_spark = spark.read.parquet(input_uri)
            # PySpark execution path on EMR: convert to pandas for transformation or execute SQL
            df_pandas = df_spark.toPandas()
            df_silver = clean_and_transform_taxi_dataframe(df_pandas)

            # Write back out via Spark
            spark_silver = spark.createDataFrame(df_silver)
            spark_silver.write.mode("overwrite").parquet(output_uri)
            logger.info("PySpark successfully wrote Silver staging records to %s", output_uri)
            return output_uri
        except Exception as exc:
            logger.warning("Spark execution encountered an issue (%s). Falling back to local engine.", exc)

    # Local Python / Pandas execution path
    raw_path = to_local_path(settings.local_lake_dir, raw_s3_key)
    df_raw = load_dataset_records(raw_path)

    df_silver = clean_and_transform_taxi_dataframe(df_raw)

    output_path = to_local_path(settings.local_lake_dir, output_filename)
    write_dataset_records(df_silver, output_path)

    logger.info(
        "Bronze to Silver transformation completed: %d silver records landed in %s",
        len(df_silver),
        output_filename,
    )
    return str(output_path)


def main() -> None:
    """CLI entry point for running Bronze to Silver job."""
    parser = argparse.ArgumentParser(description="UrbanPulse PySpark Bronze to Silver Batch ETL")
    parser.add_argument("--year", type=int, default=2024, help="Target year")
    parser.add_argument("--month", type=int, default=1, help="Target month 1-12")
    parser.add_argument("--local", action="store_true", help="Force local lake mode")

    args = parser.parse_args()
    dest = run_bronze_to_silver(year=args.year, month=args.month, force_local=args.local)
    print(f"[OK] Bronze to Silver complete: {dest}")


if __name__ == "__main__":
    main()
