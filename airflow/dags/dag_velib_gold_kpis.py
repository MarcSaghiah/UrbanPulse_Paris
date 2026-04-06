import io
import os
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

SILVER_PREFIX = "silver/velib/station_status"
GOLD_PREFIX = "gold/velib/kpis_station_snapshot"


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


def get_latest_key(client, bucket: str, prefix: str) -> str:
    paginator = client.get_paginator("list_objects_v2")
    latest_obj = None

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if latest_obj is None or obj["LastModified"] > latest_obj["LastModified"]:
                latest_obj = obj

    if latest_obj is None:
        raise FileNotFoundError(f"No objects found in s3://{bucket}/{prefix}")

    return latest_obj["Key"]


def read_parquet_from_s3(client, bucket: str, key: str) -> pd.DataFrame:
    response = client.get_object(Bucket=bucket, Key=key)
    payload = response["Body"].read()
    return pd.read_parquet(io.BytesIO(payload))


def build_kpi_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = [
        "num_bikes_available",
        "num_docks_available",
        "num_bikes_available_types.ebike",
        "num_bikes_available_types.mechanical",
        "is_renting",
        "station_id",
        "collected_at_utc",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = 0 if col != "collected_at_utc" else None

    df["num_bikes_available"] = pd.to_numeric(df["num_bikes_available"], errors="coerce").fillna(0)
    df["num_docks_available"] = pd.to_numeric(df["num_docks_available"], errors="coerce").fillna(0)
    df["num_bikes_available_types.ebike"] = pd.to_numeric(
        df["num_bikes_available_types.ebike"], errors="coerce"
    ).fillna(0)
    df["num_bikes_available_types.mechanical"] = pd.to_numeric(
        df["num_bikes_available_types.mechanical"], errors="coerce"
    ).fillna(0)
    df["is_renting"] = pd.to_numeric(df["is_renting"], errors="coerce").fillna(0)

    collected_at = pd.to_datetime(df["collected_at_utc"], utc=True, errors="coerce")
    collected_at_value = collected_at.dropna().max()
    if pd.isna(collected_at_value):
        collected_at_value = pd.Timestamp.now(tz="UTC")

    kpis = {
        "collected_at_utc": collected_at_value.isoformat(),
        "station_count": int(df["station_id"].nunique()),
        "active_station_count": int((df["is_renting"] == 1).sum()),
        "total_bikes_available": int(df["num_bikes_available"].sum()),
        "total_docks_available": int(df["num_docks_available"].sum()),
        "total_ebikes_available": int(df["num_bikes_available_types.ebike"].sum()),
        "total_mechanical_bikes_available": int(df["num_bikes_available_types.mechanical"].sum()),
    }

    out = pd.DataFrame([kpis])
    out_dt = pd.to_datetime(out["collected_at_utc"], utc=True)
    out["year"] = out_dt.dt.year.astype(int)
    out["month"] = out_dt.dt.month.astype(int)
    out["day"] = out_dt.dt.day.astype(int)
    out["hour"] = out_dt.dt.hour.astype(int)
    return out


def write_gold_parquet(client, df: pd.DataFrame, bucket: str, prefix: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    year = int(df.iloc[0]["year"])
    month = int(df.iloc[0]["month"])
    day = int(df.iloc[0]["day"])
    hour = int(df.iloc[0]["hour"])

    key = (
        f"{prefix}/year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}/"
        f"velib_kpis_{ts}.parquet"
    )

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    return key


def build_gold_kpis_snapshot() -> None:
    silver_bucket = os.getenv("S3_SILVER_BUCKET", "")
    gold_bucket = os.getenv("S3_GOLD_BUCKET", "")
    region = os.getenv("AWS_REGION", "eu-west-3")

    if not silver_bucket:
        raise ValueError("S3_SILVER_BUCKET is not set")
    if not gold_bucket:
        raise ValueError("S3_GOLD_BUCKET is not set")

    client = make_s3_client(region)
    latest_silver_key = get_latest_key(client, silver_bucket, SILVER_PREFIX)
    silver_df = read_parquet_from_s3(client, silver_bucket, latest_silver_key)
    gold_df = build_kpi_snapshot(silver_df)
    gold_key = write_gold_parquet(client, gold_df, gold_bucket, GOLD_PREFIX)

    print(f"Latest silver file: s3://{silver_bucket}/{latest_silver_key}")
    print(f"Wrote gold parquet: s3://{gold_bucket}/{gold_key}")
    print(gold_df.to_string(index=False))


default_args = {
    "owner": "marcsag",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_velib_gold_kpis",
    default_args=default_args,
    description="Builds a Gold KPI snapshot from latest Velib Silver parquet",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gold", "velib", "kpi"],
) as dag:
    build = PythonOperator(
        task_id="build_gold_kpis_snapshot",
        python_callable=build_gold_kpis_snapshot,
    )

    build
