"""
Unit tests for Airflow DAGs integrity, task dependencies, and custom operators.
"""

import importlib
import sys
import unittest
from pathlib import Path


class TestAirflowDAGIntegrity(unittest.TestCase):

    def setUp(self):
        self.project_root = Path(__file__).resolve().parent.parent.parent
        if str(self.project_root) not in sys.path:
            sys.path.insert(0, str(self.project_root))

    def test_import_ingest_sf_taxi_dag(self):
        mod = importlib.import_module("airflow.dags.ingest_sf_taxi_dag")
        self.assertTrue(hasattr(mod, "dag"))
        self.assertEqual(mod.dag.dag_id, "urbanpulse_ingest_sf_taxi")
        self.assertIn("taxi", mod.dag.kwargs.get("tags", []))

    def test_import_ingest_weather_dag(self):
        mod = importlib.import_module("airflow.dags.ingest_weather_dag")
        self.assertTrue(hasattr(mod, "dag"))
        self.assertEqual(mod.dag.dag_id, "urbanpulse_ingest_weather")
        self.assertIn("weather", mod.dag.kwargs.get("tags", []))

    def test_import_spark_batch_dag(self):
        mod = importlib.import_module("airflow.dags.spark_batch_dag")
        self.assertTrue(hasattr(mod, "dag"))
        self.assertEqual(mod.dag.dag_id, "urbanpulse_spark_batch")

    def test_import_quality_check_dag(self):
        mod = importlib.import_module("airflow.dags.quality_check_dag")
        self.assertTrue(hasattr(mod, "dag"))
        self.assertEqual(mod.dag.dag_id, "urbanpulse_quality_checks")

    def test_import_run_dbt_dag(self):
        mod = importlib.import_module("airflow.dags.run_dbt_dag")
        self.assertTrue(hasattr(mod, "dag"))
        self.assertEqual(mod.dag.dag_id, "urbanpulse_run_dbt")

    def test_import_master_e2e_dag(self):
        mod = importlib.import_module("airflow.dags.urbanpulse_end_to_end_pipeline")
        self.assertTrue(hasattr(mod, "dag"))
        self.assertEqual(mod.dag.dag_id, "urbanpulse_end_to_end_pipeline")

    def test_data_quality_operator_execution(self):
        from airflow.plugins.operators.data_quality_operator import DataQualityCheckOperator
        from ingestion.config.paths import get_raw_taxi_key

        op = DataQualityCheckOperator(
            task_id="test_dq_op",
            s3_key=get_raw_taxi_key(2024, 1),
            min_rows=1,
            not_null_columns=["trip_id"],
        )
        result = op.execute({})
        self.assertEqual(result["status"], "PASSED")
        self.assertGreaterEqual(result["row_count"], 1)

    def test_emr_step_operator_local_execution(self):
        from airflow.plugins.operators.emr_step_operator import UrbanPulseEmrStepOperator

        op = UrbanPulseEmrStepOperator(
            task_id="test_emr_op",
            job_name="Test-BronzeToSilver",
            job_script_path="spark/jobs/bronze_to_silver.py",
            job_args=["--year", "2024", "--month", "1"],
            local_fallback=True,
        )
        res = op.execute({})
        self.assertTrue(res.startswith("local-success"))


if __name__ == "__main__":
    unittest.main()
