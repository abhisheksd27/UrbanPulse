"""
UrbanPulse - Custom Airflow Operator: Lake Data Quality Checks
==============================================================
Validates dataset integrity at each stage of the pipeline (Bronze, Silver, Gold).
Checks:
  - Minimum row count thresholds
  - Null percentage thresholds on primary/critical columns
  - Numerical sanity boundaries (e.g. fare > 0, distance > 0)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

logger = logging.getLogger("airflow.task")

from airflow.plugins.airflow_compat import AirflowException, BaseOperator

from ingestion.config.paths import to_local_path
from ingestion.config.s3_storage import StorageSink
from ingestion.config.settings import settings
from spark.utils.s3_utils import load_dataset_records


class DataQualityCheckOperator(BaseOperator):
    """
    Executes data quality rule assertions against landed lake files.
    """

    template_fields: Sequence[str] = ("s3_key",)

    def __init__(
        self,
        *,
        s3_key: str,
        min_rows: int = 1,
        not_null_columns: Optional[List[str]] = None,
        positive_columns: Optional[List[str]] = None,
        max_null_pct: float = 0.05,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.s3_key = s3_key
        self.min_rows = min_rows
        self.not_null_columns = not_null_columns or []
        self.positive_columns = positive_columns or []
        self.max_null_pct = max_null_pct

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Perform validation assertions."""
        logger.info("Executing Data Quality Checks on %s...", self.s3_key)

        sink = StorageSink()
        if not sink.object_exists(self.s3_key):
            raise AirflowException(f"Quality Check Failed: Target dataset does not exist at {self.s3_key}")

        local_path = to_local_path(settings.local_lake_dir, self.s3_key)
        df = load_dataset_records(local_path)
        row_count = len(df)
        logger.info("Loaded dataset with %d rows and %d columns", row_count, len(df.columns))

        # 1. Row count check
        if row_count < self.min_rows:
            raise AirflowException(
                f"Quality Check Failed: Expected at least {self.min_rows} rows, found {row_count}"
            )

        # 2. Not-null checks
        for col in self.not_null_columns:
            if col not in df.columns:
                raise AirflowException(f"Quality Check Failed: Required column '{col}' is missing.")
            null_count = df[col].isna().sum()
            null_pct = null_count / row_count
            logger.info("Column '%s': null_count=%d (%.2f%%)", col, null_count, null_pct * 100)
            if null_pct > self.max_null_pct:
                raise AirflowException(
                    f"Quality Check Failed: Column '{col}' null rate {null_pct:.2%} exceeds max allowed {self.max_null_pct:.2%}"
                )

        # 3. Positive number checks
        for col in self.positive_columns:
            if col in df.columns:
                negative_count = (df[col] < 0).sum()
                if negative_count > 0:
                    raise AirflowException(
                        f"Quality Check Failed: Column '{col}' contains {negative_count} negative values!"
                    )

        summary = {
            "status": "PASSED",
            "s3_key": self.s3_key,
            "row_count": row_count,
            "columns_checked": self.not_null_columns + self.positive_columns,
        }
        logger.info("Data Quality Check PASSED: %s", summary)
        return summary
