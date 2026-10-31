"""
UrbanPulse Airflow Plugin
=========================
Registers custom operators with the Airflow execution environment.
"""

from airflow.plugins.operators.data_quality_operator import DataQualityCheckOperator
from airflow.plugins.operators.emr_step_operator import UrbanPulseEmrStepOperator

try:
    from airflow.plugins_manager import AirflowPlugin

    class UrbanPulsePlugin(AirflowPlugin):
        name = "urbanpulse_plugin"
        operators = [UrbanPulseEmrStepOperator, DataQualityCheckOperator]

except ImportError:
    pass

__all__ = [
    "UrbanPulseEmrStepOperator",
    "DataQualityCheckOperator",
]
