"""
DAG: dbt_raw_vault
Schedule: None (triggered by daily_ingest via TriggerDagRunOperator)
Description: Run dbt staging and raw vault models, then trigger dbt_business_vault.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="dbt_raw_vault",
    description="Run dbt staging + raw vault models (triggered by daily_ingest)",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dbt", "raw_vault", "phase4"],
) as dag:

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select staging "
            "--vars '{\"load_date\": \"{{ dag_run.conf.get(\\\"load_date\\\", ds) }}\"}'"
        ),
    )

    dbt_raw_vault_hubs = BashOperator(
        task_id="dbt_raw_vault_hubs",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select raw_vault.hubs "
            "--vars '{\"load_date\": \"{{ dag_run.conf.get(\\\"load_date\\\", ds) }}\"}'"
        ),
    )

    dbt_raw_vault_links = BashOperator(
        task_id="dbt_raw_vault_links",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select raw_vault.links "
            "--vars '{\"load_date\": \"{{ dag_run.conf.get(\\\"load_date\\\", ds) }}\"}'"
        ),
    )

    dbt_raw_vault_satellites = BashOperator(
        task_id="dbt_raw_vault_satellites",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select raw_vault.satellites "
            "--vars '{\"load_date\": \"{{ dag_run.conf.get(\\\"load_date\\\", ds) }}\"}'"
        ),
    )

    dbt_test_raw_vault = BashOperator(
        task_id="dbt_test_raw_vault",
        bash_command=f"cd {DBT_DIR} && dbt test --select raw_vault",
    )

    trigger_dbt_business_vault = TriggerDagRunOperator(
        task_id="trigger_dbt_business_vault",
        trigger_dag_id="dbt_business_vault",
        conf={"load_date": "{{ dag_run.conf.get('load_date', ds) }}"},
        wait_for_completion=False,
    )

    # Task chain: staging → hubs → links → satellites → tests → trigger next DAG
    (
        dbt_staging
        >> dbt_raw_vault_hubs
        >> dbt_raw_vault_links
        >> dbt_raw_vault_satellites
        >> dbt_test_raw_vault
        >> trigger_dbt_business_vault
    )
