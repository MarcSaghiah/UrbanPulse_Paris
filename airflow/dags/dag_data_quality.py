import io
import os
from datetime import datetime, timedelta

import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator


def resolve_aws_creds() -> tuple[str | None, str | None]:
	access_key = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY")
	secret_key = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_KEY")
	return access_key, secret_key


def make_s3_client(region: str):
	access_key, secret_key = resolve_aws_creds()
	if not access_key or not secret_key:
		raise ValueError("Missing AWS credentials in environment")
	return boto3.client(
		"s3",
		region_name=region,
		aws_access_key_id=access_key,
		aws_secret_access_key=secret_key,
	)


def latest_key(client, bucket: str, prefix: str) -> str:
	page = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
	contents = page.get("Contents", [])
	if not contents:
		raise FileNotFoundError(f"No object in s3://{bucket}/{prefix}")
	return sorted(contents, key=lambda x: x["LastModified"])[-1]["Key"]


def check_latest_gold_kpis() -> None:
	bucket = os.getenv("S3_GOLD_BUCKET", "")
	region = os.getenv("AWS_REGION", "eu-west-3")
	prefix = "gold/velib/kpis_station_snapshot"

	if not bucket:
		raise ValueError("S3_GOLD_BUCKET is not set")

	s3 = make_s3_client(region)
	key = latest_key(s3, bucket, prefix)
	body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
	df = pd.read_parquet(io.BytesIO(body))

	if df.empty:
		raise ValueError("Gold KPI dataframe is empty")

	row = df.iloc[0]
	if int(row.get("station_count", 0)) <= 0:
		raise ValueError("station_count must be > 0")
	if int(row.get("total_bikes_available", 0)) < 0:
		raise ValueError("total_bikes_available must be >= 0")
	if int(row.get("total_docks_available", 0)) < 0:
		raise ValueError("total_docks_available must be >= 0")

	print(f"Data quality checks passed for s3://{bucket}/{key}")


default_args = {
	"owner": "marcsag",
	"depends_on_past": False,
	"retries": 1,
	"retry_delay": timedelta(minutes=2),
}

with DAG(
	dag_id="dag_data_quality",
	default_args=default_args,
	start_date=datetime(2024, 1, 1),
	schedule=None,
	catchup=False,
	tags=["quality", "gold"],
) as dag:
	quality_check = PythonOperator(
		task_id="check_latest_gold_kpis",
		python_callable=check_latest_gold_kpis,
	)

	quality_check

