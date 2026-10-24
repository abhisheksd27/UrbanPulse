"""
UrbanPulse - Storage Sink Abstraction (AWS S3 & Local Lake)
===========================================================
Provides a unified interface for uploading and managing data in the S3 Data Lake.
Gracefully falls back to local directory storage when running locally, in testing,
or when AWS credentials are not configured.
"""

from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import Optional, Union

from ingestion.config.paths import to_local_path, to_s3_uri
from ingestion.config.settings import settings

logger = logging.getLogger("urbanpulse.storage")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [StorageSink] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


class StorageSink:
    """
    Dual-mode Storage Sink:
      - S3 Mode: Uploads to AWS S3 using boto3
      - Local Mode: Writes to local filesystem under settings.local_lake_dir
    """

    def __init__(
        self,
        bucket_name: Optional[str] = None,
        aws_region: Optional[str] = None,
        force_local: bool = False,
    ):
        self.bucket_name = bucket_name or settings.s3_lake_bucket
        self.aws_region = aws_region or settings.aws_region
        self.local_base_dir = settings.local_lake_dir
        self.force_local = force_local
        self._s3_client = None
        self._is_s3_available = False

        if not force_local:
            self._init_s3_client()

    def _init_s3_client(self) -> None:
        """Attempt to instantiate boto3 S3 client with available credentials."""
        try:
            import boto3
            from botocore.exceptions import ClientError, NoCredentialsError

            client = boto3.client("s3", region_name=self.aws_region)
            # Lightweight check: does caller have permission or valid credentials?
            # We don't fail immediately here; actual write operation will catch and fall back.
            self._s3_client = client
            self._is_s3_available = True
            logger.info("Connected to AWS S3 client in region %s", self.aws_region)
        except (ImportError, Exception) as exc:
            logger.info("S3 client not initialized (%s). Defaulting to local storage mode.", exc)
            self._is_s3_available = False

    @property
    def is_cloud(self) -> bool:
        """Returns True if writing to real AWS S3."""
        return self._is_s3_available and not self.force_local

    def upload_bytes(
        self,
        data: bytes,
        s3_key: str,
        content_type: str = "application/octet-stream",
    ) -> str:
        """
        Upload bytes to S3 or write to local simulated lake.
        Returns the target URI (s3://... or file://...).
        """
        s3_key = s3_key.lstrip("/")

        if self.is_cloud and self._s3_client:
            try:
                self._s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=data,
                    ContentType=content_type,
                )
                uri = to_s3_uri(self.bucket_name, s3_key)
                logger.info("Successfully uploaded %d bytes to %s", len(data), uri)
                return uri
            except Exception as exc:
                logger.warning(
                    "AWS S3 upload failed (%s). Falling back to local lake storage.", exc
                )

        # Local storage fallback
        target_path = to_local_path(self.local_base_dir, s3_key)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        with open(target_path, "wb") as f:
            f.write(data)

        local_uri = f"file://{target_path.resolve()}"
        logger.info("Wrote %d bytes to local lake: %s", len(data), target_path)
        return local_uri

    def upload_file(self, local_file_path: Union[str, Path], s3_key: str) -> str:
        """Upload a local file directly."""
        path = Path(local_file_path)
        if not path.is_file():
            raise FileNotFoundError(f"Source file not found: {path}")

        with open(path, "rb") as f:
            data = f.read()

        content_type = "application/octet-stream"
        if path.suffix == ".parquet":
            content_type = "application/vnd.apache.parquet"
        elif path.suffix == ".geojson" or path.suffix == ".json":
            content_type = "application/json"
        elif path.suffix == ".csv":
            content_type = "text/csv"

        return self.upload_bytes(data, s3_key, content_type=content_type)

    def object_exists(self, s3_key: str) -> bool:
        """
        Check if an object exists at the given key.
        Provides idempotency: avoids re-downloading existing batches.
        """
        s3_key = s3_key.lstrip("/")

        if self.is_cloud and self._s3_client:
            try:
                self._s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
                return True
            except Exception:
                pass

        # Check local filesystem
        target_path = to_local_path(self.local_base_dir, s3_key)
        return target_path.is_file() and target_path.stat().st_size > 0

    def read_bytes(self, s3_key: str) -> bytes:
        """Retrieve stored bytes from S3 or local lake."""
        s3_key = s3_key.lstrip("/")

        if self.is_cloud and self._s3_client:
            try:
                response = self._s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
                return response["Body"].read()
            except Exception as exc:
                logger.warning("Could not read from S3 (%s), trying local disk.", exc)

        target_path = to_local_path(self.local_base_dir, s3_key)
        if not target_path.is_file():
            raise FileNotFoundError(f"Object not found in S3 or local disk: {s3_key}")
        with open(target_path, "rb") as f:
            return f.read()
