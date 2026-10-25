"""
UrbanPulse - SparkSession Factory & S3A Configuration
=====================================================
Builds an optimized SparkSession for AWS EMR and local development.
Configures:
  - AWS S3A filesystem connector with DefaultAWSCredentialsProviderChain
  - Adaptive Query Execution (AQE)
  - Columnar Parquet filter pushdown
  - UTC timezone consistency
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any, Optional

logger = logging.getLogger("urbanpulse.spark_session")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [SparkSession] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def get_spark_session(
    app_name: str = "UrbanPulse-SparkJob",
    master: Optional[str] = None,
    enable_s3a: bool = True,
    aws_region: str = "us-east-1",
) -> Any:
    """
    Creates or retrieves an active SparkSession with production cloud configurations.
    Returns SparkSession if PySpark is installed; otherwise returns None with a log warning.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        logger.info(
            "PySpark is not installed in the current Python environment. "
            "Jobs can run in fallback Pandas mode for local development, "
            "or be submitted to AWS EMR via spark-submit."
        )
        return None

    builder = SparkSession.builder.appName(app_name)

    # Master: default to local[*] if not running on a YARN cluster (EMR)
    if master:
        builder = builder.master(master)
    elif "YARN_CONF_DIR" not in os.environ and "SPARK_HOME" not in os.environ:
        builder = builder.master("local[*]")

    # Performance & Optimization settings
    builder = (
        builder.config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )

    # S3A Filesystem configuration for reading/writing Amazon S3
    if enable_s3a:
        builder = (
            builder.config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            )
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            .config("spark.hadoop.fs.s3a.multipart.size", "104857600")  # 100MB
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession initialized: %s (version: %s)", app_name, spark.version)
    return spark
