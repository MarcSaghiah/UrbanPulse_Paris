from __future__ import annotations

import io
import os
from dataclasses import dataclass

import boto3
import pandas as pd
import pyarrow.parquet as pq
import streamlit as st
from dotenv import load_dotenv


GOLD_PREFIX = "gold/velib/kpis_station_snapshot"


@dataclass
class AwsConfig:
    region: str
    access_key: str | None
    secret_key: str | None
    gold_bucket: str


def load_config() -> AwsConfig:
    load_dotenv()
    return AwsConfig(
        region=os.getenv("AWS_REGION", "eu-west-3"),
        access_key=os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY"),
        secret_key=os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_KEY"),
        gold_bucket=os.getenv("S3_GOLD_BUCKET", "").strip(),
    )


def make_s3_client(cfg: AwsConfig):
    if not cfg.access_key or not cfg.secret_key:
        raise ValueError(
            "Missing AWS credentials. Set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
            "or AWS_ACCESS_KEY/AWS_SECRET_KEY in .env"
        )
    return boto3.client(
        "s3",
        region_name=cfg.region,
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
    )


def list_recent_gold_keys(client, bucket: str, prefix: str, limit: int = 200) -> list[str]:
    paginator = client.get_paginator("list_objects_v2")
    objects: list[dict] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects.extend(page.get("Contents", []))

    parquet_objects = [obj for obj in objects if str(obj.get("Key", "")).endswith(".parquet")]
    parquet_objects.sort(key=lambda x: x["LastModified"])
    return [obj["Key"] for obj in parquet_objects[-limit:]]


def read_parquet_from_s3(client, bucket: str, key: str) -> pd.DataFrame:
    response = client.get_object(Bucket=bucket, Key=key)
    payload = response["Body"].read()
    table = pq.read_table(io.BytesIO(payload))
    return table.to_pandas()


def load_gold_history(client, bucket: str, prefix: str) -> pd.DataFrame:
    keys = list_recent_gold_keys(client, bucket, prefix)
    if not keys:
        raise FileNotFoundError(
            f"No Gold files found in s3://{bucket}/{prefix}. Run dag_velib_gold_kpis first."
        )

    frames = [read_parquet_from_s3(client, bucket, key) for key in keys]
    history = pd.concat(frames, ignore_index=True)

    history["collected_at_utc"] = pd.to_datetime(history["collected_at_utc"], utc=True, errors="coerce")
    history = history.dropna(subset=["collected_at_utc"]).sort_values("collected_at_utc")

    history["active_station_ratio"] = history["active_station_count"] / history["station_count"].replace(0, pd.NA)
    history["active_station_ratio"] = history["active_station_ratio"].fillna(0.0)

    history["ebike_share"] = history["total_ebikes_available"] / history["total_bikes_available"].replace(0, pd.NA)
    history["ebike_share"] = history["ebike_share"].fillna(0.0)

    return history


def render_dashboard(df: pd.DataFrame, bucket: str) -> None:
    st.title("Urban Pulse Paris Dashboard")
    st.caption(f"Source: s3://{bucket}/{GOLD_PREFIX}")

    latest = df.iloc[-1]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Station count", int(latest["station_count"]))
    c2.metric("Active stations", int(latest["active_station_count"]))
    c3.metric("Bikes available", int(latest["total_bikes_available"]))
    c4.metric("Docks available", int(latest["total_docks_available"]))

    st.subheader("Time Series")

    st.markdown("Bikes available over time")
    st.line_chart(df.set_index("collected_at_utc")["total_bikes_available"])

    st.markdown("Active station ratio over time")
    st.line_chart(df.set_index("collected_at_utc")["active_station_ratio"])

    st.markdown("E-bike share over time")
    st.line_chart(df.set_index("collected_at_utc")["ebike_share"])

    st.subheader("Latest Snapshot")
    view_cols = [
        "collected_at_utc",
        "station_count",
        "active_station_count",
        "total_bikes_available",
        "total_docks_available",
        "total_ebikes_available",
        "total_mechanical_bikes_available",
        "active_station_ratio",
        "ebike_share",
    ]
    st.dataframe(df[view_cols].tail(20), use_container_width=True)


def main() -> None:
    st.set_page_config(page_title="Urban Pulse Paris", layout="wide")

    cfg = load_config()
    if not cfg.gold_bucket:
        st.error("S3_GOLD_BUCKET is missing in .env")
        st.stop()

    try:
        client = make_s3_client(cfg)
        history = load_gold_history(client, cfg.gold_bucket, GOLD_PREFIX)
        render_dashboard(history, cfg.gold_bucket)
    except Exception as exc:
        st.error(str(exc))
        st.info("Check .env credentials and run dag_velib_gold_kpis before opening the dashboard.")


if __name__ == "__main__":
    main()
