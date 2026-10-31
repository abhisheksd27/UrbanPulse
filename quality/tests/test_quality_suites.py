"""
==============================================================================
UrbanPulse Data Quality & Great Expectations Unit Tests
==============================================================================
Validates:
- Expectation suite JSON file integrity and syntax.
- Checkpoint YAML definitions.
- Evaluation engine against valid data lake records.
- Evaluation engine error detection when given invalid records.
==============================================================================
"""

import json
import re
import unittest
from pathlib import Path
import pandas as pd

try:
    import yaml
except ImportError:
    yaml = None

from quality.run_checks import (
    EXPECTATIONS_DIR,
    QUALITY_DIR,
    QualityEvaluator,
    run_all_checks,
    run_suite_validation,
)

CHECKPOINTS_DIR = QUALITY_DIR / "checkpoints"


class TestQualitySuites(unittest.TestCase):
    def setUp(self):
        self.suites = ["sf_taxi_raw", "weather_raw", "sf_taxi_staging", "sf_taxi_curated"]
        self.checkpoints = [
            "taxi_bronze_checkpoint.yml",
            "taxi_silver_checkpoint.yml",
            "taxi_gold_checkpoint.yml",
        ]

    def test_expectation_suites_files_exist_and_valid_json(self):
        """Ensures all 4 expectation suite JSON files exist and have required GE properties."""
        for suite_name in self.suites:
            suite_file = EXPECTATIONS_DIR / f"{suite_name}.json"
            self.assertTrue(suite_file.exists(), f"Suite file missing: {suite_file}")

            with open(suite_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            self.assertEqual(data["expectation_suite_name"], suite_name)
            self.assertIn("expectations", data)
            self.assertGreater(len(data["expectations"]), 0)

            for exp in data["expectations"]:
                self.assertIn("expectation_type", exp)
                self.assertIn("kwargs", exp)

    def test_checkpoint_yamls_exist_and_valid(self):
        """Ensures checkpoint YAML configs are valid and point to correct suites."""
        for cp_file in self.checkpoints:
            path = CHECKPOINTS_DIR / cp_file
            self.assertTrue(path.exists(), f"Checkpoint YAML missing: {path}")

            with open(path, "r", encoding="utf-8") as f:
                content = f.read()

            if yaml:
                config = yaml.safe_load(content)
                self.assertIn("name", config)
                self.assertIn("validations", config)
                suite = config["validations"][0].get("expectation_suite_name")
                self.assertIn(suite, self.suites)
            else:
                self.assertIn("name:", content)
                self.assertIn("expectation_suite_name:", content)
                match = re.search(r"expectation_suite_name:\s*([a-zA-Z0-9_]+)", content)
                self.assertIsNotNone(match)
                self.assertIn(match.group(1), self.suites)

    def test_run_all_checks_passes_on_lake_data(self):
        """Tests that all 4 expectation suites pass against the current data lake files."""
        overall_success, reports = run_all_checks()
        self.assertTrue(overall_success, "Expected all suites to pass on lake data")
        self.assertEqual(len(reports), 4)

        for r in reports:
            self.assertTrue(r["success"], f"Suite {r['suite_name']} failed validation")
            self.assertEqual(r["failed_expectations"], 0)

    def test_quality_evaluator_detects_nulls_and_bounds_violations(self):
        """Tests evaluator logic catches deliberately injected errors."""
        df_faulty = pd.DataFrame({
            "trip_id": ["T1", None, "T3"],
            "fare_amount": [15.0, -5.0, 30.0],
            "payment_type": ["Cash", "InvalidType", "Cash"],
        })

        evaluator = QualityEvaluator(df_faulty, "test_suite", {})

        # 1. Null check should fail on row 2
        r_null = evaluator.evaluate_expectation({
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "trip_id"},
        })
        self.assertFalse(r_null["success"])
        self.assertEqual(r_null["details"]["null_count"], 1)

        # 2. Bound check should fail on -5.0 fare
        r_bound = evaluator.evaluate_expectation({
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "fare_amount", "min_value": 0.0, "max_value": 100.0},
        })
        self.assertFalse(r_bound["success"])
        self.assertEqual(r_bound["observed_value"]["violations"], 1)

        # 3. Set check should fail on 'InvalidType'
        r_set = evaluator.evaluate_expectation({
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {"column": "payment_type", "value_set": ["Cash", "Credit Card"]},
        })
        self.assertFalse(r_set["success"])


if __name__ == "__main__":
    unittest.main()
