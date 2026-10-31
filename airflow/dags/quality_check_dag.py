"""
DAG: urbanpulse_quality_checks
==============================
Standalone Data Quality Verification Suite.
Validates all data layers (Bronze, Silver, Gold facts, Gold marts) across:
  - Row counts
  - Missing or null keys
  - Numerical constraints (fares, distances, speeds, durations)
  - Join completeness (weather conditions)
"""

from __future__ import annotations

import datetime

from airflow.plugins.airflow_compat import (
    DAG,
    EmptyOperator,
)

from airflow.plugins.operators.data_quality_operator import DataQualityCheckOperator
from ingestion.config.paths import get_curated_taxi_prefix, get_raw_taxi_key, get_raw_weather_key, get_staging_taxi_prefix
from ingestion.config.settings import settings

default_args = {
    "owner": "urbanpulse-data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
}

with DAG(
    dag_id="urbanpulse_quality_checks",
    default_args=default_args,
    description="Cross-layer data quality assertions on S3 Data Lake",
    schedule_interval=None,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=["urbanpulse", "quality", "governance"],
) as dag:

    start = EmptyOperator(task_id="start")

    # 1. Bronze Quality
    check_bronze_taxi = DataQualityCheckOperator(
        task_id="check_bronze_taxi",
        s3_key=get_raw_taxi_key(2024, 1),
        min_rows=1,
        not_null_columns=["trip_id", "pickup_datetime"],
    )

    check_bronze_weather = DataQualityCheckOperator(
        task_id="check_bronze_weather",
        s3_key=get_raw_weather_key(2024, 1),
        min_rows=672,
        not_null_columns=["time", "temperature_2m"],
    )

    # 2. Silver Quality
    check_silver_trips = DataQualityCheckOperator(
        task_id="check_silver_trips",
        s3_key=f"{get_staging_taxi_prefix(2024, 1)}part-00000.parquet",
        min_rows=1,
        not_null_columns=["trip_id", "pickup_at", "dropoff_at", "pickup_neighborhood"],
        positive_columns=["fare_amount", "distance_km", "duration_minutes"],
    )

    # 3. Gold Quality
    check_gold_facts = DataQualityCheckOperator(
        task_id="check_gold_facts",
        s3_key=f"{get_curated_taxi_prefix(2024, 1)}fact_trips.parquet",
        min_rows=1,
        not_null_columns=["trip_id", "pickup_neighborhood", "weather_condition"],
        positive_columns=["fare_amount", "total_amount", "speed_kmh"],
    )

    check_gold_mart = DataQualityCheckOperator(
        task_id="check_gold_mart",
        s3_key=f"{settings.s3_curated_prefix}/marts/daily_ops/year=2024/month=01/daily_ops.parquet",
        min_rows=1,
        not_null_columns=["pickup_date", "pickup_neighborhood"],
        positive_columns=["total_trips", "total_revenue"],
    )

    end = EmptyOperator(task_id="end")

    start >> [check_bronze_taxi, check_bronze_weather] >> check_silver_trips >> [check_gold_facts, check_gold_mart] >> end
