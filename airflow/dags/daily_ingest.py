"""
DAG: daily_ingest
Schedule: 02:00 UTC daily
Description: Ingest wearable simulator output → MinIO landing zone, then trigger dbt_raw_vault.
"""

from __future__ import annotations

import io
import os
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "")
BUCKET = "raw"


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def verify_files_uploaded(ds: str, **kwargs) -> None:
    """Check that readings.json and alerts.json exist in the landing zone."""
    year, month, day = ds.split("-")
    prefix = f"wearable/{year}/{month}/{day}/"
    required_files = ["readings.json", "alerts.json"]

    s3 = _s3_client()
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    existing_keys = {
        obj["Key"].split("/")[-1]
        for obj in response.get("Contents", [])
    }

    missing = [f for f in required_files if f not in existing_keys]
    if missing:
        raise AirflowException(
            f"Missing files in s3://{BUCKET}/{prefix}: {missing}. "
            f"Found: {existing_keys}"
        )

    print(f"All required files present in s3://{BUCKET}/{prefix}: {required_files}")


def verify_row_counts(ds: str, **kwargs) -> None:
    """Download readings.json and verify it contains at least 100 lines."""
    year, month, day = ds.split("-")
    key = f"wearable/{year}/{month}/{day}/readings.json"
    min_rows = 100

    s3 = _s3_client()
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
    except s3.exceptions.NoSuchKey:
        raise AirflowException(f"readings.json not found at s3://{BUCKET}/{key}")

    content = obj["Body"].read().decode("utf-8")
    line_count = sum(1 for line in content.splitlines() if line.strip())

    print(f"readings.json line count for {ds}: {line_count}")
    if line_count < min_rows:
        raise AirflowException(
            f"readings.json has only {line_count} rows (minimum expected: {min_rows}). "
            f"Check simulator output for {ds}."
        )

    print(f"Row count sanity check passed: {line_count} >= {min_rows}")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="daily_ingest",
    description="Ingest wearable simulator output → MinIO landing zone",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ingest", "wearable", "phase4"],
) as dag:

    generate_wearable_data = BashOperator(
        task_id="generate_wearable_data",
        bash_command=(
            "python -m simulator.cli generate-day "
            "--date {{ ds }} "
            "--upload "
            "--seed 42"
        ),
    )

    verify_files_uploaded_task = PythonOperator(
        task_id="verify_files_uploaded",
        python_callable=verify_files_uploaded,
        op_kwargs={"ds": "{{ ds }}"},
    )

    verify_row_counts_task = PythonOperator(
        task_id="verify_row_counts",
        python_callable=verify_row_counts,
        op_kwargs={"ds": "{{ ds }}"},
    )

    trigger_dbt_raw_vault = TriggerDagRunOperator(
        task_id="trigger_dbt_raw_vault",
        trigger_dag_id="dbt_raw_vault",
        conf={"load_date": "{{ ds }}"},
        wait_for_completion=False,
    )

    # Task dependencies
    (
        generate_wearable_data
        >> verify_files_uploaded_task
        >> verify_row_counts_task
        >> trigger_dbt_raw_vault
    )
