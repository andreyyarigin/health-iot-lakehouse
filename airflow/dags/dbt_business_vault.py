"""
DAG: dbt_business_vault
Schedule: None (triggered by dbt_raw_vault via TriggerDagRunOperator)
Description: Run dbt business vault and mart models, then notify completion.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "depends_on_past": False,
}

DBT_DIR = "/opt/airflow/dbt_project"

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def notify_complete(**kwargs) -> None:
    """Log pipeline completion message."""
    load_date = kwargs["dag_run"].conf.get("load_date") or kwargs["ds"]
    log.info(
        "Pipeline complete for %s, ready for ML. "
        "mart_patient_daily_features is up to date.",
        load_date,
    )
    print(f"Pipeline complete for {load_date}, ready for ML")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="dbt_business_vault",
    description="Run dbt business vault + mart models (triggered by dbt_raw_vault)",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dbt", "business_vault", "marts", "phase4"],
) as dag:

    dbt_business_vault = BashOperator(
        task_id="dbt_business_vault",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select business_vault "
            "--vars '{\"load_date\": \"{{ dag_run.conf.get(\\\"load_date\\\", ds) }}\"}'"
        ),
    )

    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select marts "
            "--vars '{\"load_date\": \"{{ dag_run.conf.get(\\\"load_date\\\", ds) }}\"}'"
        ),
    )

    dbt_test_all = BashOperator(
        task_id="dbt_test_all",
        bash_command=f"cd {DBT_DIR} && dbt test",
    )

    notify_complete_task = PythonOperator(
        task_id="notify_complete",
        python_callable=notify_complete,
    )

    # Task chain: business_vault → marts → test_all → notify
    (
        dbt_business_vault
        >> dbt_marts
        >> dbt_test_all
        >> notify_complete_task
    )
