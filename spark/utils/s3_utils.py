"""
UrbanPulse - Spark & Data Lake Path / IO Helpers
================================================
Provides IO helpers for PySpark and fallback Pandas engines:
  - Translates s3:// URIs to s3a:// for Hadoop / Spark compatibility
  - Handles local simulated lake filesystem paths
  - Reads/writes Parquet and JSON files safely
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Any, Optional, Union

import pandas as pd

from ingestion.config.paths import format_year_month, to_local_path
from ingestion.config.settings import settings

logger = logging.getLogger("urbanpulse.s3_utils")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [SparkIO] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def resolve_spark_path(s3_key: str, force_local: bool = False) -> str:
    """
    Resolve an S3 key to the appropriate Spark/EMR or local URI.
    If writing to AWS S3: returns 's3a://<bucket>/<key>'
    If local/offline: returns absolute local file path string.
    """
    s3_key = s3_key.lstrip("/")
    if not force_local and os.getenv("AWS_EXECUTION_ENV"):
        return f"s3a://{settings.s3_lake_bucket}/{s3_key}"

    # Default to local simulated lake path if local directory exists
    local_path = to_local_path(settings.local_lake_dir, s3_key)
    if force_local or local_path.parent.exists() or local_path.exists():
        return str(local_path.resolve())

    # Fallback to S3A URI
    return f"s3a://{settings.s3_lake_bucket}/{s3_key}"


def load_dataset_records(file_path: Union[str, Path]) -> pd.DataFrame:
    """
    Robust reader for local lake files: reads Parquet or Gzip-compressed JSON / CSV.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Dataset path does not exist: {path}")

    # If it's a directory (e.g. Spark output folder with part-* files)
    if path.is_dir():
        parquet_files = list(path.glob("*.parquet"))
        json_files = list(path.glob("*.json*"))
        if parquet_files:
            try:
                return pd.read_parquet(path)
            except Exception:
                dfs = [load_dataset_records(f) for f in parquet_files]
                return pd.concat(dfs, ignore_index=True)
        elif json_files:
            dfs = [load_dataset_records(f) for f in json_files]
            return pd.concat(dfs, ignore_index=True)

    # 1. Try reading as native Parquet
    try:
        return pd.read_parquet(path)
    except Exception:
        pass

    # 2. Check if file is gzip-compressed (magic bytes 0x1f 0x8b)
    is_gzip = False
    try:
        with open(path, "rb") as f:
            magic = f.read(2)
            if magic == b"\x1f\x8b":
                is_gzip = True
    except Exception:
        pass

    # 3. Read as JSON (with or without gzip)
    try:
        compression = "gzip" if is_gzip else "infer"
        return pd.read_json(path, compression=compression)
    except Exception:
        pass

    # 4. Fallback to CSV
    try:
        return pd.read_csv(path, compression="gzip" if is_gzip else "infer")
    except Exception as exc:
        raise ValueError(f"Could not read dataset file at {path}: {exc}")


def write_dataset_records(df: pd.DataFrame, output_path: Union[str, Path]) -> str:
    """
    Writes DataFrame to target path (Parquet or compressed JSON).
    """
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    try:
        df.to_parquet(path, index=False, engine="auto", compression="snappy")
        logger.info("Saved %d records to Parquet: %s", len(df), path)
        return str(path)
    except Exception as exc:
        # Fallback to compressed JSON
        logger.info("Parquet engine not available (%s). Writing JSON: %s", exc, path)
        df.to_json(path, orient="records", indent=2)
        return str(path)
