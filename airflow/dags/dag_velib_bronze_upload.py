import os
from datetime import datetime, timedelta
from pathlib import Path

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator

OUTPUT_DIR = Path("/opt/airflow/data/raw/velib")
S3_PREFIX = "bronze/velib/status"


def resolve_aws_creds() -> tuple[str | None, str | None]:
    access_key = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_KEY")
    return access_key, secret_key


def find_latest_snapshot(input_dir: Path) -> Path:
    files = sorted(input_dir.glob("velib_status_*.json"))
    if not files:
        raise FileNotFoundError(f"No Velib snapshot found in {input_dir}")
    return files[-1]


def upload_latest_snapshot_to_s3() -> None:
    bucket = os.getenv("S3_BRONZE_BUCKET", "")
    region = os.getenv("AWS_REGION", "eu-west-3")
    access_key, secret_key = resolve_aws_creds()

    if not bucket:
        raise ValueError("S3_BRONZE_BUCKET is not set in environment")

    if not access_key or not secret_key:
        raise ValueError(
            "Missing AWS credentials in environment. Set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
            "or AWS_ACCESS_KEY/AWS_SECRET_KEY."
        )

    latest_file = find_latest_snapshot(OUTPUT_DIR)
    s3_key = f"{S3_PREFIX}/{latest_file.name}"

    client = boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    client.upload_file(str(latest_file), bucket, s3_key)

    print(f"Uploaded {latest_file} to s3://{bucket}/{s3_key}")


default_args = {
    "owner": "marcsag",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_velib_bronze_upload",
    default_args=default_args,
    description="Uploads latest local Velib snapshot to S3 Bronze",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ingestion", "velib", "bronze", "s3"],
) as dag:
    upload = PythonOperator(
        task_id="upload_latest_snapshot_to_s3",
        python_callable=upload_latest_snapshot_to_s3,
    )

    upload
