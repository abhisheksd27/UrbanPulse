"""
UrbanPulse Ingestion Configuration Package
"""

from ingestion.config.paths import (
    format_year_month,
    get_curated_taxi_prefix,
    get_raw_geo_key,
    get_raw_taxi_key,
    get_raw_weather_key,
    get_staging_taxi_prefix,
    to_local_path,
    to_s3_uri,
)
from ingestion.config.s3_storage import StorageSink
from ingestion.config.settings import IngestionSettings, settings

__all__ = [
    "settings",
    "IngestionSettings",
    "StorageSink",
    "get_raw_taxi_key",
    "get_raw_weather_key",
    "get_raw_geo_key",
    "get_staging_taxi_prefix",
    "get_curated_taxi_prefix",
    "to_s3_uri",
    "to_local_path",
    "format_year_month",
]
