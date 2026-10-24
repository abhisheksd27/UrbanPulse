"""
UrbanPulse - Lake Path & Partition Conventions
==============================================
Centralizes naming patterns and URI generation for S3 data lake layers:
  - raw/     (Bronze)
  - staging/ (Silver)
  - curated/ (Gold)
"""

from __future__ import annotations

from pathlib import Path
from typing import Union

from ingestion.config.settings import settings


def format_year_month(year: Union[int, str], month: Union[int, str]) -> tuple[str, str]:
    """Ensure 4-digit year and 2-digit zero-padded month."""
    y = str(year).strip()
    m = f"{int(month):02d}"
    return y, m


def get_raw_taxi_key(year: Union[int, str], month: Union[int, str], filename: str = "trips.parquet") -> str:
    """
    Generate the S3 key for raw SF taxi trip Parquet data.
    Pattern: raw/taxi/year=YYYY/month=MM/trips.parquet
    """
    y, m = format_year_month(year, month)
    return f"{settings.s3_raw_prefix}/taxi/year={y}/month={m}/{filename}"


def get_raw_weather_key(year: Union[int, str], month: Union[int, str], filename: str = "weather.parquet") -> str:
    """
    Generate the S3 key for raw Open-Meteo weather Parquet data.
    Pattern: raw/weather/year=YYYY/month=MM/weather.parquet
    """
    y, m = format_year_month(year, month)
    return f"{settings.s3_raw_prefix}/weather/year={y}/month={m}/{filename}"


def get_raw_geo_key(filename: str = "sf_neighborhoods.geojson") -> str:
    """
    Generate the S3 key for static SF Neighborhood geospatial boundaries.
    Pattern: raw/geo/sf_neighborhoods.geojson
    """
    return f"{settings.s3_raw_prefix}/geo/{filename}"


def get_staging_taxi_prefix(year: Union[int, str], month: Union[int, str]) -> str:
    """
    Generate S3 prefix for Silver staging taxi data.
    Pattern: staging/taxi/year=YYYY/month=MM/
    """
    y, m = format_year_month(year, month)
    return f"{settings.s3_staging_prefix}/taxi/year={y}/month={m}/"


def get_curated_taxi_prefix(year: Union[int, str], month: Union[int, str]) -> str:
    """
    Generate S3 prefix for Gold curated taxi data.
    Pattern: curated/taxi/year=YYYY/month=MM/
    """
    y, m = format_year_month(year, month)
    return f"{settings.s3_curated_prefix}/taxi/year={y}/month={m}/"


def to_s3_uri(bucket: str, key: str) -> str:
    """Convert bucket and key to standard s3:// URI format."""
    clean_key = key.lstrip("/")
    return f"s3://{bucket}/{clean_key}"


def to_local_path(base_dir: Path, key: str) -> Path:
    """Resolve a simulated lake key into a local filesystem path."""
    return base_dir / key
