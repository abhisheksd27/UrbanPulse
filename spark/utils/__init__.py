"""
UrbanPulse Spark Utilities
==========================
Utilities:
  - spark_session: Configures SparkSession with S3A and AQE for EMR and local
  - s3_utils: Resolves S3/local URIs and reads/writes Parquet and JSON datasets
"""

from spark.utils.s3_utils import load_dataset_records, resolve_spark_path, write_dataset_records
from spark.utils.spark_session import get_spark_session

__all__ = [
    "get_spark_session",
    "resolve_spark_path",
    "load_dataset_records",
    "write_dataset_records",
]
