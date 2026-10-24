"""
UrbanPulse - Ingestion Configuration Settings
=============================================
Manages typed configuration parameters loaded from environment variables and .env files.
Provides sensible production defaults with local dev fallbacks.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


def _load_dotenv_fallback(dotenv_path: Path) -> None:
    """Fallback .env file reader if python-dotenv is not installed."""
    if not dotenv_path.is_file():
        return
    try:
        with open(dotenv_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip().strip("'\"")
                if key and key not in os.environ:
                    os.environ[key] = val
    except Exception:
        pass


# Attempt to load .env from project root
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_ENV_FILE = _PROJECT_ROOT / ".env"

try:
    from dotenv import load_dotenv

    load_dotenv(_ENV_FILE)
except ImportError:
    _load_dotenv_fallback(_ENV_FILE)


@dataclass
class IngestionSettings:
    """Strongly-typed settings for UrbanPulse data ingestion."""

    # Environment
    environment: str = field(
        default_factory=lambda: os.getenv("ENVIRONMENT", "dev").lower()
    )
    project_name: str = field(
        default_factory=lambda: os.getenv("PROJECT_NAME", "urbanpulse")
    )

    # AWS S3 Storage
    aws_region: str = field(
        default_factory=lambda: os.getenv("AWS_REGION", "us-east-1")
    )
    s3_lake_bucket: str = field(
        default_factory=lambda: os.getenv("S3_LAKE_BUCKET", "urbanpulse-lake-dev")
    )
    s3_artifacts_bucket: str = field(
        default_factory=lambda: os.getenv(
            "S3_ARTIFACTS_BUCKET", "urbanpulse-artifacts-dev"
        )
    )

    # S3 Lake Prefixes (Medallion Layers)
    s3_raw_prefix: str = field(
        default_factory=lambda: os.getenv("S3_RAW_PREFIX", "raw")
    )
    s3_staging_prefix: str = field(
        default_factory=lambda: os.getenv("S3_STAGING_PREFIX", "staging")
    )
    s3_curated_prefix: str = field(
        default_factory=lambda: os.getenv("S3_CURATED_PREFIX", "curated")
    )

    # Local storage simulation directory (used when AWS S3 is unavailable or in offline mode)
    local_lake_dir: Path = field(
        default_factory=lambda: Path(
            os.getenv("LOCAL_LAKE_DIR", str(_PROJECT_ROOT / "data" / "lake"))
        )
    )

    # DataSF Open Data Portal API (Taxi Trips)
    datasf_base_url: str = field(
        default_factory=lambda: os.getenv(
            "DATASF_BASE_URL", "https://data.sfgov.org/resource/yitu-d5am.json"
        )
    )
    datasf_app_token: Optional[str] = field(
        default_factory=lambda: os.getenv("DATASF_APP_TOKEN")
    )

    # Open-Meteo Historical Weather API
    openmeteo_base_url: str = field(
        default_factory=lambda: os.getenv(
            "OPENMETEO_BASE_URL", "https://archive-api.open-meteo.com/v1/archive"
        )
    )

    # San Francisco Coordinates (Center / City Hall)
    sf_latitude: float = field(
        default_factory=lambda: float(os.getenv("SF_LATITUDE", "37.7749"))
    )
    sf_longitude: float = field(
        default_factory=lambda: float(os.getenv("SF_LONGITUDE", "-122.4194"))
    )

    # Kafka / Streaming Settings
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    kafka_taxi_topic: str = field(
        default_factory=lambda: os.getenv("KAFKA_TAXI_TOPIC", "sf-taxi-events")
    )
    kafka_weather_topic: str = field(
        default_factory=lambda: os.getenv("KAFKA_WEATHER_TOPIC", "sf-weather-events")
    )

    # Redshift Credentials
    redshift_host: str = field(
        default_factory=lambda: os.getenv("REDSHIFT_HOST", "localhost")
    )
    redshift_port: int = field(
        default_factory=lambda: int(os.getenv("REDSHIFT_PORT", "5439"))
    )
    redshift_db: str = field(
        default_factory=lambda: os.getenv("REDSHIFT_DB", "urbanpulse_dw")
    )
    redshift_user: str = field(
        default_factory=lambda: os.getenv("REDSHIFT_USER", "admin")
    )

    # Pagination & Request defaults
    default_batch_limit: int = 50000
    request_timeout_seconds: int = 60
    max_retries: int = 3


# Global singleton instance
settings = IngestionSettings()
