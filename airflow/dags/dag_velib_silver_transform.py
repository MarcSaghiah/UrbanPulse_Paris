import os
import io
from datetime import datetime, timedelta
from datetime import timezone

import boto3
import pandas as pd
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator


BRONZE_PREFIX = "bronze/velib/status"
SILVER_PREFIX = "silver/velib/station_status"


def resolve_aws_creds() -> tuple[str | None, str | None]:
    access_key = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_KEY")
    return access_key, secret_key


def make_s3_client(region: str):
    access_key, secret_key = resolve_aws_creds()
    if not access_key or not secret_key:
        raise ValueError(
            "Missing AWS credentials. Set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
            "or AWS_ACCESS_KEY/AWS_SECRET_KEY in environment."
        )

    return boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def get_latest_bronze_key(client, bucket: str, prefix: str) -> str:
    paginator = client.get_paginator("list_objects_v2")
    latest_obj = None

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if latest_obj is None or obj["LastModified"] > latest_obj["LastModified"]:
                latest_obj = obj

    if latest_obj is None:
        raise FileNotFoundError(f"No objects found in s3://{bucket}/{prefix}")

    return latest_obj["Key"]


def read_json_from_s3(client, bucket: str, key: str) -> dict[str, Any]:
    response = client.get_object(Bucket=bucket, Key=key)
    payload = response["Body"].read().decode("utf-8")
    return pd.read_json(io.StringIO(payload), typ="series").to_dict()


def normalize_stations(data: dict[str, Any]) -> pd.DataFrame:
    stations = data.get("data", {}).get("stations", [])
    if not stations:
        raise ValueError("No stations found in bronze payload")

    df = pd.json_normalize(stations)

    collected_at = data.get("_metadata", {}).get("collected_at_utc")
    if collected_at:
        collected_at_dt = pd.to_datetime(collected_at, utc=True, errors="coerce")
    else:
        collected_at_dt = pd.Timestamp.now(tz="UTC")

    if pd.isna(collected_at_dt):
        collected_at_dt = pd.Timestamp.now(tz="UTC")

    df["collected_at_utc"] = collected_at_dt.isoformat()
    df["year"] = int(collected_at_dt.year)
    df["month"] = int(collected_at_dt.month)
    df["day"] = int(collected_at_dt.day)
    df["hour"] = int(collected_at_dt.hour)

    keep_cols = [
        "station_id",
        "is_installed",
        "is_renting",
        "is_returning",
        "last_reported",
        "numBikesAvailable",
        "numDocksAvailable",
        "num_bikes_available",
        "num_docks_available",
        "num_bikes_available_types.ebike",
        "num_bikes_available_types.mechanical",
        "collected_at_utc",
        "year",
        "month",
        "day",
        "hour",
    ]

    for col in keep_cols:
        if col not in df.columns:
            df[col] = None

    return df[keep_cols]


def write_parquet_to_s3(client, df: pd.DataFrame, bucket: str, prefix: str) -> str:
    if df.empty:
        raise ValueError("Dataframe is empty, nothing to write")

    year = int(df.iloc[0]["year"])
    month = int(df.iloc[0]["month"])
    day = int(df.iloc[0]["day"])
    hour = int(df.iloc[0]["hour"])

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = (
        f"{prefix}/year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}/"
        f"velib_station_status_{ts}.parquet"
    )

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    return key


def run_transform(bronze_bucket: str, silver_bucket: str, region: str) -> None:
    client = make_s3_client(region)
    latest_key = get_latest_bronze_key(client, bronze_bucket, BRONZE_PREFIX)
    data = read_json_from_s3(client, bronze_bucket, latest_key)
    df = normalize_stations(data)
    silver_key = write_parquet_to_s3(client, df, silver_bucket, SILVER_PREFIX)

    print(f"Latest bronze file: s3://{bronze_bucket}/{latest_key}")
    print(f"Wrote silver parquet: s3://{silver_bucket}/{silver_key}")
    print(f"Rows written: {len(df)}")


def transform_velib_bronze_to_silver() -> None:
    bronze_bucket = os.getenv("S3_BRONZE_BUCKET", "")
    silver_bucket = os.getenv("S3_SILVER_BUCKET", "")
    region = os.getenv("AWS_REGION", "eu-west-3")

    if not bronze_bucket:
        raise ValueError("S3_BRONZE_BUCKET is not set")
    if not silver_bucket:
        raise ValueError("S3_SILVER_BUCKET is not set")

    run_transform(bronze_bucket=bronze_bucket, silver_bucket=silver_bucket, region=region)


default_args = {
    "owner": "marcsag",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_velib_silver_transform",
    default_args=default_args,
    description="Transforms latest Velib bronze JSON to partitioned silver Parquet",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["silver", "velib", "transform"],
) as dag:
    transform = PythonOperator(
        task_id="transform_velib_bronze_to_silver",
        python_callable=transform_velib_bronze_to_silver,
    )

    transform
