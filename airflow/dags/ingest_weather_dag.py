"""
DAG: urbanpulse_ingest_weather
==============================
Fetches historical hourly meteorological observations for San Francisco
from Open-Meteo Archive API and lands Parquet records into S3 Raw (Bronze Zone).

Schedule: Monthly on the 2nd at 01:00 UTC (precedes taxi ingestion)
Upstream: Open-Meteo API
Downstream: Consumed by Spark Silver to Gold job for weather joining
"""

from __future__ import annotations

import datetime
from typing import Any, Dict

from airflow.plugins.airflow_compat import (
    DAG,
    EmptyOperator,
    PythonOperator,
)

from airflow.plugins.operators.data_quality_operator import DataQualityCheckOperator
from ingestion.batch.weather_ingester import SFWeatherBatchIngester
from ingestion.config.paths import get_raw_weather_key
from ingestion.config.s3_storage import StorageSink


def _run_weather_ingestion(**context: Dict[str, Any]) -> str:
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}

    now = datetime.datetime.now(datetime.timezone.utc)
    year = conf.get("year", now.year)
    month = conf.get("month", now.month)
    force = conf.get("force", False)

    sink = StorageSink()
    ingester = SFWeatherBatchIngester(storage_sink=sink, use_sample_data=False)
    dest_uri = ingester.ingest_month(year=int(year), month=int(month), force=bool(force))
    return dest_uri


default_args = {
    "owner": "urbanpulse-data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=5),
}

with DAG(
    dag_id="urbanpulse_ingest_weather",
    default_args=default_args,
    description="Fetches hourly SF weather and lands in S3 Bronze zone",
    schedule_interval="0 1 2 * *",  # Monthly on the 2nd day at 01:00 UTC
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["urbanpulse", "ingestion", "bronze", "weather"],
) as dag:

    start = EmptyOperator(task_id="start")

    ingest_weather_data = PythonOperator(
        task_id="ingest_weather_data",
        python_callable=_run_weather_ingestion,
        provide_context=True,
    )

    validate_bronze_weather = DataQualityCheckOperator(
        task_id="validate_bronze_weather",
        s3_key=get_raw_weather_key(2024, 1),
        min_rows=672,  # Minimum 28 days * 24 hours
        not_null_columns=["time", "temperature_2m"],
    )

    end = EmptyOperator(task_id="end")

    start >> ingest_weather_data >> validate_bronze_weather >> end
