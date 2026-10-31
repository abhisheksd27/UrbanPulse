"""
UrbanPulse - Custom Airflow Operator: EMR Step Execution
========================================================
Submits PySpark transformation steps to an Amazon EMR cluster and monitors
execution until completion or failure with exponential backoff polling.

Includes local dev fallback: if no EMR cluster is active, runs the job
via the local Python/PySpark runner.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, List, Optional, Sequence

logger = logging.getLogger("airflow.task")

from airflow.plugins.airflow_compat import AirflowException, BaseOperator


class UrbanPulseEmrStepOperator(BaseOperator):
    """
    Submits a PySpark step to an Amazon EMR cluster and waits for completion.
    """

    template_fields: Sequence[str] = ("cluster_id", "job_script_path", "job_args")

    def __init__(
        self,
        *,
        cluster_id: Optional[str] = None,
        job_name: str,
        job_script_path: str,
        job_args: Optional[List[str]] = None,
        aws_conn_id: str = "aws_default",
        region_name: str = "us-east-1",
        poll_interval: int = 15,
        max_retries: int = 40,
        local_fallback: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster_id = cluster_id or os.getenv("EMR_CLUSTER_ID")
        self.job_name = job_name
        self.job_script_path = job_script_path
        self.job_args = job_args or []
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.poll_interval = poll_interval
        self.max_retries = max_retries
        self.local_fallback = local_fallback

    def execute(self, context: Dict[str, Any]) -> str:
        """Execute the step on EMR or run locally if in dev mode."""
        logger.info("Executing Step: %s", self.job_name)

        if not self.cluster_id or self.cluster_id.startswith("j-REPLACE"):
            if self.local_fallback:
                logger.info(
                    "No active EMR cluster specified. Executing in local runner mode: %s %s",
                    self.job_script_path,
                    " ".join(self.job_args),
                )
                return self._execute_locally()
            raise AirflowException("No EMR cluster ID specified and local_fallback is False.")

        return self._execute_on_emr()

    def _execute_on_emr(self) -> str:
        """Submit and poll step on AWS EMR via boto3."""
        try:
            import boto3

            emr_client = boto3.client("emr", region_name=self.region_name)
        except Exception as exc:
            raise AirflowException(f"Failed to initialize boto3 EMR client: {exc}")

        step_args = ["spark-submit", "--deploy-mode", "client", self.job_script_path] + self.job_args
        step_config = [
            {
                "Name": self.job_name,
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": step_args,
                },
            }
        ]

        logger.info("Submitting step to EMR cluster %s: %s", self.cluster_id, step_config)
        response = emr_client.add_job_flow_steps(
            JobFlowId=self.cluster_id, Steps=step_config
        )
        step_ids = response.get("StepIds", [])
        if not step_ids:
            raise AirflowException("EMR step submission failed: No step ID returned.")

        step_id = step_ids[0]
        logger.info("Step submitted successfully. Step ID: %s", step_id)

        # Polling loop
        for attempt in range(self.max_retries):
            time.sleep(self.poll_interval)
            step_status_resp = emr_client.describe_step(
                ClusterId=self.cluster_id, StepId=step_id
            )
            state = step_status_resp["Step"]["Status"]["State"]
            logger.info("Polling EMR Step [%s]: State = %s (Attempt %d/%d)", step_id, state, attempt + 1, self.max_retries)

            if state == "COMPLETED":
                logger.info("EMR Step %s completed successfully!", step_id)
                return step_id
            elif state in ["FAILED", "CANCELLED"]:
                failure_reason = step_status_resp["Step"]["Status"].get("FailureDetails", {}).get("Reason", "Unknown")
                raise AirflowException(f"EMR Step {step_id} ended in {state}: {failure_reason}")

        raise AirflowException(f"EMR Step {step_id} timed out after {self.max_retries * self.poll_interval}s.")

    def _execute_locally(self) -> str:
        """Executes the Python/Spark script locally using the appropriate runner."""
        import importlib
        import sys

        # Convert path to module if needed
        script = self.job_script_path
        if "bronze_to_silver" in script:
            from spark.jobs.bronze_to_silver import run_bronze_to_silver
            year = 2024
            month = 1
            for i, arg in enumerate(self.job_args):
                if arg == "--year" and i + 1 < len(self.job_args):
                    year = int(self.job_args[i + 1])
                elif arg == "--month" and i + 1 < len(self.job_args):
                    month = int(self.job_args[i + 1])
            res = run_bronze_to_silver(year=year, month=month, force_local=True)
            return f"local-success: {res}"

        elif "silver_to_gold" in script:
            from spark.jobs.silver_to_gold import run_silver_to_gold
            year = 2024
            month = 1
            for i, arg in enumerate(self.job_args):
                if arg == "--year" and i + 1 < len(self.job_args):
                    year = int(self.job_args[i + 1])
                elif arg == "--month" and i + 1 < len(self.job_args):
                    month = int(self.job_args[i + 1])
            facts, mart = run_silver_to_gold(year=year, month=month, force_local=True)
            return f"local-success: facts={facts}, mart={mart}"

        return "local-noop"
