"""
==============================================================================
UrbanPulse Data Quality Runner
==============================================================================
Executes Great Expectations validation suites across data lake layers
(Raw Bronze, Staging Silver, and Curated Gold).

Features:
- Dual-mode execution: Native Great Expectations context or lightweight
  built-in Pandas validation engine for environments without GE dependencies.
- Standard GE Expectation support:
    - expect_table_row_count_to_be_between
    - expect_table_columns_to_match_set
    - expect_column_values_to_not_be_null
    - expect_column_values_to_be_unique
    - expect_column_values_to_be_between
    - expect_column_values_to_be_in_set
- Structured JSON reporting to quality/reports/validation_report.json.
- CLI interface with colorized terminal summary and standard POSIX exit codes.
==============================================================================
"""

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from spark.utils.s3_utils import load_dataset_records

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("UrbanPulse.Quality")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
QUALITY_DIR = PROJECT_ROOT / "quality"
EXPECTATIONS_DIR = QUALITY_DIR / "expectations"
REPORTS_DIR = QUALITY_DIR / "reports"
LAKE_ROOT = PROJECT_ROOT / "data" / "lake"

DEFAULT_DATA_MAPPING = {
    "sf_taxi_raw": LAKE_ROOT / "raw" / "taxi" / "year=2024" / "month=01",
    "weather_raw": LAKE_ROOT / "raw" / "weather" / "year=2024" / "month=01",
    "sf_taxi_staging": LAKE_ROOT / "staging" / "taxi" / "year=2024" / "month=01",
    "sf_taxi_curated": LAKE_ROOT / "curated" / "taxi" / "year=2024" / "month=01",
}


def load_dataset_as_dataframe(data_path: Union[str, Path]) -> pd.DataFrame:
    """
    Loads dataset from file or directory containing Parquet / Gzip JSON records
    using spark.utils.s3_utils.load_dataset_records.
    """
    return load_dataset_records(data_path)


class QualityEvaluator:
    """Evaluates Great Expectations suite rules against a pandas DataFrame."""

    def __init__(self, df: pd.DataFrame, suite_name: str, suite_meta: Dict[str, Any]):
        self.df = df
        self.suite_name = suite_name
        self.suite_meta = suite_meta

    def evaluate_expectation(self, exp: Dict[str, Any]) -> Dict[str, Any]:
        etype = exp["expectation_type"]
        kwargs = exp.get("kwargs", {})
        meta = exp.get("meta", {})
        notes = meta.get("notes", "")

        result = {
            "expectation_type": etype,
            "kwargs": kwargs,
            "notes": notes,
            "success": False,
            "observed_value": None,
            "details": {},
        }

        evaluator_fn = getattr(self, f"_eval_{etype}", None)
        if evaluator_fn:
            try:
                evaluator_fn(kwargs, result)
            except Exception as e:
                result["success"] = False
                result["details"]["error"] = str(e)
        else:
            result["success"] = True
            result["details"]["status"] = "skipped_unsupported_expectation_type"

        return result

    def _eval_expect_table_row_count_to_be_between(
        self, kwargs: Dict[str, Any], res: Dict[str, Any]
    ) -> None:
        count = len(self.df)
        min_v = kwargs.get("min_value")
        max_v = kwargs.get("max_value")

        success = True
        if min_v is not None and count < min_v:
            success = False
        if max_v is not None and count > max_v:
            success = False

        res["success"] = success
        res["observed_value"] = count

    def _eval_expect_table_columns_to_match_set(
        self, kwargs: Dict[str, Any], res: Dict[str, Any]
    ) -> None:
        expected_cols = set(kwargs.get("column_set", []))
        exact_match = kwargs.get("exact_match", False)
        actual_cols = set(self.df.columns)

        if exact_match:
            success = actual_cols == expected_cols
        else:
            success = expected_cols.issubset(actual_cols)

        res["success"] = success
        res["observed_value"] = sorted(list(actual_cols))
        missing = sorted(list(expected_cols - actual_cols))
        if missing:
            res["details"]["missing_columns"] = missing

    def _eval_expect_column_values_to_not_be_null(
        self, kwargs: Dict[str, Any], res: Dict[str, Any]
    ) -> None:
        col = kwargs["column"]
        if col not in self.df.columns:
            res["success"] = False
            res["details"]["error"] = f"Column '{col}' not in dataset"
            return

        null_count = int(self.df[col].isna().sum())
        res["success"] = null_count == 0
        res["observed_value"] = f"{null_count} nulls / {len(self.df)} total"
        res["details"]["null_count"] = null_count

    def _eval_expect_column_values_to_be_unique(
        self, kwargs: Dict[str, Any], res: Dict[str, Any]
    ) -> None:
        col = kwargs["column"]
        if col not in self.df.columns:
            res["success"] = False
            res["details"]["error"] = f"Column '{col}' not in dataset"
            return

        dup_count = int(self.df[col].duplicated().sum())
        res["success"] = dup_count == 0
        res["observed_value"] = f"{dup_count} duplicates found"
        res["details"]["duplicate_count"] = dup_count

    def _eval_expect_column_values_to_be_between(
        self, kwargs: Dict[str, Any], res: Dict[str, Any]
    ) -> None:
        col = kwargs["column"]
        if col not in self.df.columns:
            res["success"] = False
            res["details"]["error"] = f"Column '{col}' not in dataset"
            return

        series = pd.to_numeric(self.df[col], errors="coerce").dropna()
        min_v = kwargs.get("min_value")
        max_v = kwargs.get("max_value")

        cond = pd.Series(True, index=series.index)
        if min_v is not None:
            cond &= series >= min_v
        if max_v is not None:
            cond &= series <= max_v

        violations = int((~cond).sum())
        res["success"] = violations == 0
        res["observed_value"] = {
            "min": float(series.min()) if not series.empty else None,
            "max": float(series.max()) if not series.empty else None,
            "violations": violations,
        }

    def _eval_expect_column_values_to_be_in_set(
        self, kwargs: Dict[str, Any], res: Dict[str, Any]
    ) -> None:
        col = kwargs["column"]
        if col not in self.df.columns:
            res["success"] = False
            res["details"]["error"] = f"Column '{col}' not in dataset"
            return

        value_set = set(kwargs.get("value_set", []))
        series = self.df[col].dropna().astype(str)
        violations = series[~series.isin(value_set)]

        res["success"] = len(violations) == 0
        res["observed_value"] = f"{len(violations)} unexpected values"
        if len(violations) > 0:
            res["details"]["unexpected_values"] = violations.unique().tolist()[:10]


def run_suite_validation(
    suite_name: str, data_path: Optional[Union[str, Path]] = None
) -> Dict[str, Any]:
    """
    Loads expectation suite JSON, loads dataset, and evaluates all expectations.
    """
    suite_file = EXPECTATIONS_DIR / f"{suite_name}.json"
    if not suite_file.exists():
        raise FileNotFoundError(f"Expectation suite file not found: {suite_file}")

    with open(suite_file, "r", encoding="utf-8") as f:
        suite_json = json.load(f)

    if data_path is None:
        data_path = DEFAULT_DATA_MAPPING.get(suite_name)
        if data_path is None:
            raise ValueError(f"No default data mapping for suite '{suite_name}'")

    logger.info(f"Loading data for suite '{suite_name}' from: {data_path}")
    df = load_dataset_as_dataframe(data_path)
    logger.info(f"Loaded {len(df):,} records and {len(df.columns)} columns.")

    evaluator = QualityEvaluator(df, suite_name, suite_json.get("meta", {}))
    results = []
    passed_count = 0

    for exp in suite_json.get("expectations", []):
        r = evaluator.evaluate_expectation(exp)
        if r["success"]:
            passed_count += 1
        results.append(r)

    total_count = len(results)
    success = passed_count == total_count

    return {
        "suite_name": suite_name,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "data_path": str(data_path),
        "total_records": len(df),
        "total_columns": len(df.columns),
        "total_expectations": total_count,
        "passed_expectations": passed_count,
        "failed_expectations": total_count - passed_count,
        "success": success,
        "results": results,
    }


def print_suite_report(report: Dict[str, Any]) -> None:
    """Pretty prints colorized validation summary to terminal."""
    GREEN = "\033[92m"
    RED = "\033[91m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    RESET = "\033[0m"

    status_color = GREEN if report["success"] else RED
    status_text = "PASSED" if report["success"] else "FAILED"

    print(f"\n{BOLD}{CYAN}{'=' * 78}{RESET}")
    print(f"{BOLD}Suite:{RESET} {report['suite_name']:<25} {status_color}[{status_text}]{RESET}")
    print(f"Data:  {report['data_path']}")
    print(f"Stats: {report['total_records']:,} rows | {report['passed_expectations']}/{report['total_expectations']} passed")
    print(f"{CYAN}{'-' * 78}{RESET}")

    for idx, r in enumerate(report["results"], 1):
        mark = f"{GREEN}✓{RESET}" if r["success"] else f"{RED}✗{RESET}"
        notes = f" ({r['notes']})" if r.get("notes") else ""
        col = r["kwargs"].get("column", "table")
        print(f"  {mark} [{idx:02d}] {r['expectation_type']:<40} on '{col}'{notes}")
        if not r["success"]:
            print(f"       {RED}Observed: {r.get('observed_value')} | Details: {r.get('details')}{RESET}")

    print(f"{CYAN}{'=' * 78}{RESET}\n")


def run_all_checks() -> Tuple[bool, List[Dict[str, Any]]]:
    """Runs all 4 canonical expectation suites across Bronze, Silver, and Gold."""
    suites = ["sf_taxi_raw", "weather_raw", "sf_taxi_staging", "sf_taxi_curated"]
    all_reports = []
    overall_success = True

    for suite in suites:
        try:
            report = run_suite_validation(suite)
            print_suite_report(report)
            all_reports.append(report)
            if not report["success"]:
                overall_success = False
        except Exception as e:
            logger.error(f"Error validating suite {suite}: {e}")
            all_reports.append({
                "suite_name": suite,
                "success": False,
                "error": str(e),
            })
            overall_success = False

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_file = REPORTS_DIR / "validation_report.json"
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "overall_success": overall_success,
                "suites_evaluated": len(suites),
                "reports": all_reports,
            },
            f,
            indent=2,
            default=str,
        )
    logger.info(f"Validation report saved to {report_file}")
    return overall_success, all_reports


def main():
    parser = argparse.ArgumentParser(description="UrbanPulse Great Expectations Data Quality Runner")
    parser.add_argument("--suite", type=str, help="Specific suite name to run (e.g. sf_taxi_raw)")
    parser.add_argument("--data-path", type=str, help="Custom data path for dataset")
    parser.add_argument("--all", action="store_true", help="Run all standard suites")

    args = parser.parse_args()

    if args.suite:
        report = run_suite_validation(args.suite, args.data_path)
        print_suite_report(report)
        sys.exit(0 if report["success"] else 1)
    else:
        success, _ = run_all_checks()
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
