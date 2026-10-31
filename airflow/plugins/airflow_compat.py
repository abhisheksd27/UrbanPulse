"""
UrbanPulse - Airflow Compatibility Layer
========================================
Provides robust BaseOperator and DAG implementations when apache-airflow is
either installed or running in a standalone testing environment.
Implements complete >> and << bitshift chaining and list handling.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Sequence, Union

logger = logging.getLogger("urbanpulse.airflow_compat")

try:
    from airflow import DAG
    from airflow.exceptions import AirflowException
    from airflow.models import BaseOperator
    from airflow.operators.bash import BashOperator
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    AIRFLOW_INSTALLED = True

except ImportError:
    AIRFLOW_INSTALLED = False

    class AirflowException(Exception):
        """Base Airflow exception shim."""
        pass

    class BaseOperator:
        """Shim for Airflow BaseOperator implementing bitshift dependencies."""

        template_fields: Sequence[str] = ()

        def __init__(self, task_id: str, **kwargs):
            self.task_id = task_id
            self.kwargs = kwargs
            self.upstream: List[BaseOperator] = []
            self.downstream: List[BaseOperator] = []

        def set_downstream(self, other: Union[BaseOperator, Sequence[BaseOperator]]) -> None:
            if isinstance(other, (list, tuple, set)):
                for o in other:
                    self.set_downstream(o)
            elif isinstance(other, BaseOperator):
                if other not in self.downstream:
                    self.downstream.append(other)
                if self not in other.upstream:
                    other.upstream.append(self)

        def set_upstream(self, other: Union[BaseOperator, Sequence[BaseOperator]]) -> None:
            if isinstance(other, (list, tuple, set)):
                for o in other:
                    self.set_upstream(o)
            elif isinstance(other, BaseOperator):
                if other not in self.upstream:
                    self.upstream.append(other)
                if self not in other.downstream:
                    other.downstream.append(self)

        def __rshift__(self, other: Union[BaseOperator, Sequence[BaseOperator]]) -> Union[BaseOperator, Sequence[BaseOperator]]:
            self.set_downstream(other)
            return other

        def __lshift__(self, other: Union[BaseOperator, Sequence[BaseOperator]]) -> Union[BaseOperator, Sequence[BaseOperator]]:
            self.set_upstream(other)
            return other

        def __rrshift__(self, other: Union[BaseOperator, Sequence[BaseOperator]]) -> BaseOperator:
            self.set_upstream(other)
            return self

        def __rlshift__(self, other: Union[BaseOperator, Sequence[BaseOperator]]) -> BaseOperator:
            self.set_downstream(other)
            return self

        def execute(self, context: Dict[str, Any]) -> Any:
            return None

    class EmptyOperator(BaseOperator):
        pass

    class BashOperator(BaseOperator):
        def __init__(self, task_id: str, bash_command: str, **kwargs):
            super().__init__(task_id=task_id, **kwargs)
            self.bash_command = bash_command

    class PythonOperator(BaseOperator):
        def __init__(self, task_id: str, python_callable: Any, **kwargs):
            super().__init__(task_id=task_id, **kwargs)
            self.python_callable = python_callable

        def execute(self, context: Dict[str, Any]) -> Any:
            if callable(self.python_callable):
                return self.python_callable(**context)
            return None

    class TriggerDagRunOperator(BaseOperator):
        def __init__(self, task_id: str, trigger_dag_id: str, **kwargs):
            super().__init__(task_id=task_id, **kwargs)
            self.trigger_dag_id = trigger_dag_id

    class DAG:
        """Shim for Airflow DAG class."""

        def __init__(self, dag_id: str, **kwargs):
            self.dag_id = dag_id
            self.kwargs = kwargs
            self.tasks: Dict[str, BaseOperator] = {}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def add_task(self, task: BaseOperator) -> None:
            self.tasks[task.task_id] = task


__all__ = [
    "DAG",
    "BaseOperator",
    "EmptyOperator",
    "PythonOperator",
    "BashOperator",
    "TriggerDagRunOperator",
    "AirflowException",
    "AIRFLOW_INSTALLED",
]
