"""
DAG: urbanpulse_spark_batch
===========================
Orchestrates PySpark batch transformations across the Medallion lakehouse:
  1. Bronze to Silver: Schema enforcement, deduplication, cleaning, and spatial enrichment
  2. Silver to Gold: Hourly weather join, KPI computation, fact & daily ops mart generation

Schedule: Triggered externally by ingestion DAGs (or manual/backfill)
Execution: AWS EMR Cluster (or local runner fallback)
"""

from __future__ import annotations

import datetime

from airflow.plugins.airflow_compat import (
    DAG,
    EmptyOperator,
    TriggerDagRunOperator,
)

from airflow.plugins.operators.data_quality_operator import DataQualityCheckOperator
from airflow.plugins.operators.emr_step_operator import UrbanPulseEmrStepOperator
from ingestion.config.paths import get_curated_taxi_prefix, get_staging_taxi_prefix
from ingestion.config.settings import settings

default_args = {
    "owner": "urbanpulse-data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=3),
}

with DAG(
    dag_id="urbanpulse_spark_batch",
    default_args=default_args,
    description="Submits and monitors EMR PySpark Bronze->Silver and Silver->Gold ETL steps",
    schedule_interval=None,  # Triggered by upstream DAGs or manual run
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["urbanpulse", "spark", "emr", "silver", "gold"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Step 1: Bronze -> Silver PySpark Job
    spark_bronze_to_silver = UrbanPulseEmrStepOperator(
        task_id="spark_bronze_to_silver",
        job_name="UrbanPulse-BronzeToSilver-Taxi",
        job_script_path="spark/jobs/bronze_to_silver.py",
        job_args=["--year", "{{ dag_run.conf.get('year', 2024) }}", "--month", "{{ dag_run.conf.get('month', 1) }}"],
        local_fallback=True,
    )

    # Step 2: Validate Silver Staging Output
    staging_file = f"{get_staging_taxi_prefix(2024, 1)}part-00000.parquet"
    validate_silver_landing = DataQualityCheckOperator(
        task_id="validate_silver_landing",
        s3_key=staging_file,
        min_rows=1,
        not_null_columns=["trip_id", "pickup_at", "dropoff_at", "pickup_neighborhood"],
        positive_columns=["fare_amount", "distance_km", "duration_minutes"],
    )

    # Step 3: Silver -> Gold PySpark Job
    spark_silver_to_gold = UrbanPulseEmrStepOperator(
        task_id="spark_silver_to_gold",
        job_name="UrbanPulse-SilverToGold-Curated",
        job_script_path="spark/jobs/silver_to_gold.py",
        job_args=["--year", "{{ dag_run.conf.get('year', 2024) }}", "--month", "{{ dag_run.conf.get('month', 1) }}"],
        local_fallback=True,
    )

    # Step 4: Validate Gold Curated Facts & Daily Ops Mart
    curated_fact_file = f"{get_curated_taxi_prefix(2024, 1)}fact_trips.parquet"
    validate_gold_facts = DataQualityCheckOperator(
        task_id="validate_gold_facts",
        s3_key=curated_fact_file,
        min_rows=1,
        not_null_columns=["trip_id", "pickup_neighborhood", "weather_condition"],
        positive_columns=["fare_amount", "total_amount", "speed_kmh"],
    )

    curated_mart_file = f"{settings.s3_curated_prefix}/marts/daily_ops/year=2024/month=01/daily_ops.parquet"
    validate_gold_mart = DataQualityCheckOperator(
        task_id="validate_gold_mart",
        s3_key=curated_mart_file,
        min_rows=1,
        not_null_columns=["pickup_date", "pickup_neighborhood"],
        positive_columns=["total_trips", "total_revenue"],
    )

    # Step 5: Trigger dbt Transformations in Redshift
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="urbanpulse_run_dbt",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> spark_bronze_to_silver
        >> validate_silver_landing
        >> spark_silver_to_gold
        >> validate_gold_facts
        >> validate_gold_mart
        >> trigger_dbt
        >> end
    )
