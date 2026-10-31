"""
DAG: urbanpulse_ingest_sf_taxi
==============================
Monthly batch ingestion of San Francisco permitted taxi trip data from DataSF (SFMTA).
Lands cleaned Parquet files into S3 Raw (Bronze Zone).

Schedule: Monthly on the 2nd at 02:00 UTC (e.g. 0 2 2 * *)
Upstream: DataSF Socrata Open Data Portal API
Downstream: Triggers urbanpulse_spark_batch DAG upon successful landing and validation
"""

from __future__ import annotations

import datetime
from typing import Any, Dict

from airflow.plugins.airflow_compat import (
    DAG,
    EmptyOperator,
    PythonOperator,
    TriggerDagRunOperator,
)

from airflow.plugins.operators.data_quality_operator import DataQualityCheckOperator
from ingestion.batch.sf_taxi_loader import SFTaxiBatchLoader
from ingestion.config.paths import get_raw_taxi_key
from ingestion.config.s3_storage import StorageSink


def _run_taxi_ingestion(**context: Dict[str, Any]) -> str:
    """Extracts target year/month from DAG run conf or execution date."""
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}

    # Default to current or previous month
    now = datetime.datetime.now(datetime.timezone.utc)
    year = conf.get("year", now.year)
    month = conf.get("month", now.month)
    force = conf.get("force", False)

    sink = StorageSink()
    loader = SFTaxiBatchLoader(storage_sink=sink, use_sample_data=False)
    dest_uri = loader.ingest_month(year=int(year), month=int(month), force=bool(force))
    return dest_uri


default_args = {
    "owner": "urbanpulse-data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=5),
}

with DAG(
    dag_id="urbanpulse_ingest_sf_taxi",
    default_args=default_args,
    description="Extracts monthly SF taxi trip data and lands in S3 Bronze zone",
    schedule_interval="0 2 2 * *",  # Monthly on the 2nd day at 02:00 UTC
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["urbanpulse", "ingestion", "bronze", "taxi"],
) as dag:

    start = EmptyOperator(task_id="start")

    ingest_taxi_data = PythonOperator(
        task_id="ingest_taxi_data",
        python_callable=_run_taxi_ingestion,
        provide_context=True,
    )

    validate_bronze_taxi = DataQualityCheckOperator(
        task_id="validate_bronze_taxi",
        s3_key=get_raw_taxi_key(2024, 1),
        min_rows=1,
        not_null_columns=["trip_id", "pickup_datetime"],
        positive_columns=["fare", "distance"],
    )

    trigger_spark_job = TriggerDagRunOperator(
        task_id="trigger_spark_job",
        trigger_dag_id="urbanpulse_spark_batch",
        conf={"year": 2024, "month": 1},
        reset_dag_run=True,
        wait_for_completion=False,
    )

    end = EmptyOperator(task_id="end")

    start >> ingest_taxi_data >> validate_bronze_taxi >> trigger_spark_job >> end
