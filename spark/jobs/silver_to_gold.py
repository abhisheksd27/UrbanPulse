"""
UrbanPulse - PySpark Job: Silver to Gold (Curated)
==================================================
Joins cleaned Silver taxi trips with hourly weather observations, calculates
advanced analytical metrics (speed, unit economics, tip percentage), and generates
both fine-grained curated fact tables and aggregated operational marts.

Execution:
  - AWS EMR:   spark-submit --master yarn spark/jobs/silver_to_gold.py --year 2024 --month 1
  - Local Dev: python -m spark.jobs.silver_to_gold --year 2024 --month 1 --local

Transformations Applied:
  1. Weather Observation Join (hourly temporal alignment on pickup_at)
  2. WMO Weather Code translation (e.g. 0=Clear, 45=Fog, 61=Rain)
  3. Efficiency & Revenue Metrics (speed_kmh, fare_per_km, tip_percentage)
  4. Generation of Curated Fact Trips: curated/taxi/year=YYYY/month=MM/
  5. Generation of Aggregated Daily Ops Mart: curated/marts/daily_ops/year=YYYY/month=MM/
"""

from __future__ import annotations

import argparse
import datetime
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from ingestion.config.paths import format_year_month, get_curated_taxi_prefix, get_raw_weather_key, get_staging_taxi_prefix, to_local_path
from ingestion.config.s3_storage import StorageSink
from ingestion.config.settings import settings
from spark.jobs.schema_definitions import GOLD_FACT_TRIPS_COLUMNS
from spark.utils.s3_utils import load_dataset_records, resolve_spark_path, write_dataset_records
from spark.utils.spark_session import get_spark_session

logger = logging.getLogger("urbanpulse.silver_to_gold")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [SilverToGold] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# WMO Weather Code dictionary mapping
WMO_CODE_MAP = {
    0: "Clear Sky",
    1: "Mainly Clear",
    2: "Partly Cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing Rime Fog",
    51: "Light Drizzle",
    53: "Moderate Drizzle",
    55: "Dense Drizzle",
    61: "Slight Rain",
    63: "Moderate Rain",
    65: "Heavy Rain",
    71: "Slight Snow",
    73: "Moderate Snow",
    75: "Heavy Snow",
    80: "Rain Showers",
    95: "Thunderstorm",
}


def translate_weather_code(code: Any) -> str:
    """Translates WMO integer code to descriptive English."""
    try:
        c = int(code)
        return WMO_CODE_MAP.get(c, "Moderate Conditions")
    except Exception:
        return "Unknown"


def enrich_and_curate_trips(df_silver: pd.DataFrame, df_weather: pd.DataFrame) -> pd.DataFrame:
    """
    Joins Silver taxi records with hourly weather observations and calculates
    speed, efficiency metrics, and tip percentages.
    """
    if df_silver.empty:
        return pd.DataFrame(columns=list(GOLD_FACT_TRIPS_COLUMNS.keys()))

    logger.info("Enriching %d Silver taxi trips with meteorological observations...", len(df_silver))
    df = df_silver.copy()

    # 1. Truncate pickup timestamp to hour for join key
    df["pickup_at_dt"] = pd.to_datetime(df["pickup_at"], errors="coerce", utc=True)
    df["join_hour"] = df["pickup_at_dt"].dt.floor("h")

    # 2. Prepare weather lookup table
    weather_lookup = {}
    if not df_weather.empty:
        w_df = df_weather.copy()
        w_df["weather_dt"] = pd.to_datetime(w_df["time"], errors="coerce", utc=True).dt.floor("h")
        for _, w_row in w_df.iterrows():
            if pd.notna(w_row["weather_dt"]):
                weather_lookup[w_row["weather_dt"]] = {
                    "temperature_c": float(w_row.get("temperature_2m", 15.0)),
                    "precipitation_mm": float(w_row.get("precipitation", 0.0)),
                    "weather_code": int(w_row.get("weathercode", 0)),
                }

    # 3. Join weather
    temps, precips, conds, rains = [], [], [], []
    for _, row in df.iterrows():
        hour_key = row["join_hour"]
        w_info = weather_lookup.get(hour_key)
        if w_info:
            temp = w_info["temperature_c"]
            precip = w_info["precipitation_mm"]
            wcode = w_info["weather_code"]
        else:
            temp = 14.5  # SF typical mean
            precip = 0.0
            wcode = 0

        temps.append(temp)
        precips.append(precip)
        conds.append(translate_weather_code(wcode))
        rains.append(precip > 0.1)

    df["temperature_c"] = temps
    df["precipitation_mm"] = precips
    df["weather_condition"] = conds
    df["is_raining"] = rains

    # 4. Calculate Derived Metrics
    # Speed in km/h = distance_km / (duration_minutes / 60)
    hours = df["duration_minutes"] / 60.0
    df["speed_kmh"] = (df["distance_km"] / hours.replace(0, pd.NA)).fillna(0.0).round(2)
    # Cap absurd speeds (GPS errors)
    df["speed_kmh"] = df["speed_kmh"].clip(lower=0.0, upper=120.0)

    # Unit economics: Fare per kilometer
    df["fare_per_km"] = (df["fare_amount"] / df["distance_km"].replace(0, pd.NA)).fillna(0.0).round(2)

    # Tip percentage = (tip / fare) * 100
    df["tip_percentage"] = (
        (df["tip_amount"] / df["fare_amount"].replace(0, pd.NA) * 100.0)
        .fillna(0.0)
        .round(2)
    )

    df["curated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    cols = [c for c in GOLD_FACT_TRIPS_COLUMNS.keys() if c in df.columns]
    return df[cols]


def compute_daily_ops_mart(df_curated: pd.DataFrame) -> pd.DataFrame:
    """
    Computes pre-aggregated operational mart by (pickup_date, pickup_neighborhood).
    """
    if df_curated.empty:
        return pd.DataFrame()

    logger.info("Computing Daily Ops Mart aggregations across %d curated trips...", len(df_curated))

    grouped = df_curated.groupby(["pickup_date", "pickup_neighborhood"], as_index=False).agg(
        total_trips=("trip_id", "count"),
        total_revenue=("total_amount", "sum"),
        avg_fare=("fare_amount", "mean"),
        avg_tip=("tip_amount", "mean"),
        avg_duration_min=("duration_minutes", "mean"),
        avg_distance_km=("distance_km", "mean"),
        avg_speed_kmh=("speed_kmh", "mean"),
        rainy_trips=("is_raining", lambda x: int(sum(x))),
        airport_trips=("is_airport_trip", lambda x: int(sum(x))),
    )

    # Round aggregations
    grouped["total_revenue"] = grouped["total_revenue"].round(2)
    grouped["avg_fare"] = grouped["avg_fare"].round(2)
    grouped["avg_tip"] = grouped["avg_tip"].round(2)
    grouped["avg_duration_min"] = grouped["avg_duration_min"].round(2)
    grouped["avg_distance_km"] = grouped["avg_distance_km"].round(2)
    grouped["avg_speed_kmh"] = grouped["avg_speed_kmh"].round(2)

    grouped["created_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    return grouped


def run_silver_to_gold(
    year: int,
    month: int,
    force_local: bool = False,
) -> Tuple[str, str]:
    """
    Executes the Silver to Gold curation and mart generation pipeline.
    """
    y_str, m_str = format_year_month(year, month)
    staging_prefix = get_staging_taxi_prefix(year, month)
    weather_key = get_raw_weather_key(year, month)

    curated_fact_prefix = get_curated_taxi_prefix(year, month)
    curated_fact_key = f"{curated_fact_prefix}fact_trips.parquet"
    curated_mart_key = f"{settings.s3_curated_prefix}/marts/daily_ops/year={y_str}/month={m_str}/daily_ops.parquet"

    logger.info("Executing Silver to Gold Job for %s-%s", y_str, m_str)
    logger.info("Input Staging Taxi Prefix: %s", staging_prefix)
    logger.info("Input Raw Weather Key:     %s", weather_key)
    logger.info("Output Fact Trips Key:     %s", curated_fact_key)
    logger.info("Output Daily Mart Key:     %s", curated_mart_key)

    sink = StorageSink(force_local=force_local)

    # 1. Load Staging Taxi Data
    staging_file = f"{staging_prefix}part-00000.parquet"
    if not sink.object_exists(staging_file):
        raise FileNotFoundError(
            f"Silver staging taxi data not found at {staging_file}. "
            f"Run Step 3 (bronze_to_silver) first."
        )

    staging_path = to_local_path(settings.local_lake_dir, staging_file)
    df_silver = load_dataset_records(staging_path)

    # 2. Load Weather Data (if present, else empty DataFrame)
    weather_path = to_local_path(settings.local_lake_dir, weather_key)
    df_weather = pd.DataFrame()
    if weather_path.exists():
        try:
            df_weather = load_dataset_records(weather_path)
            logger.info("Loaded %d weather observation records for join.", len(df_weather))
        except Exception as exc:
            logger.warning("Could not read weather data (%s), proceeding without weather.", exc)

    # 3. Transform to Gold Fact Trips
    df_gold_facts = enrich_and_curate_trips(df_silver, df_weather)

    # 4. Generate Mart Aggregations
    df_daily_mart = compute_daily_ops_mart(df_gold_facts)

    # 5. Persist Curated Datasets
    fact_dest_path = to_local_path(settings.local_lake_dir, curated_fact_key)
    write_dataset_records(df_gold_facts, fact_dest_path)

    mart_dest_path = to_local_path(settings.local_lake_dir, curated_mart_key)
    write_dataset_records(df_daily_mart, mart_dest_path)

    logger.info("Gold Curation complete: Facts -> %s, Mart -> %s", fact_dest_path, mart_dest_path)
    return str(fact_dest_path), str(mart_dest_path)


def main() -> None:
    """CLI entry point for running Silver to Gold job."""
    parser = argparse.ArgumentParser(description="UrbanPulse PySpark Silver to Gold Curated ETL")
    parser.add_argument("--year", type=int, default=2024, help="Target year")
    parser.add_argument("--month", type=int, default=1, help="Target month 1-12")
    parser.add_argument("--local", action="store_true", help="Force local lake mode")

    args = parser.parse_args()
    facts_uri, mart_uri = run_silver_to_gold(year=args.year, month=args.month, force_local=args.local)
    print(f"[OK] Curated Fact Trips: {facts_uri}")
    print(f"[OK] Daily Ops Mart:     {mart_uri}")


if __name__ == "__main__":
    main()
