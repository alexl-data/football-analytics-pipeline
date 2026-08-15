"""Verification DAG for the Step 2 Airflow setup.

Not part of the pipeline — confirms the scheduler picks up DAGs and the
LocalExecutor can run parallel tasks. Safe to delete once the real
football_pipeline_dag exists.
"""
from datetime import datetime

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id="setup_healthcheck",
    description="Verifies the Airflow setup (scheduler, executor, DB) works end to end.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["setup", "temporary"],
)
def setup_healthcheck():
    start = EmptyOperator(task_id="start")
    parallel_a = EmptyOperator(task_id="parallel_task_a")
    parallel_b = EmptyOperator(task_id="parallel_task_b")
    end = EmptyOperator(task_id="end")

    start >> [parallel_a, parallel_b] >> end


setup_healthcheck()
