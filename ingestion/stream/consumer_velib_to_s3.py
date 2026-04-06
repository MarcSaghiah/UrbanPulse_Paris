from __future__ import annotations

import io
import json
import os
from datetime import datetime, timezone

import boto3
from kafka import KafkaConsumer
from dotenv import load_dotenv


def resolve_aws_creds() -> tuple[str | None, str | None]:
    access_key = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_KEY")
    return access_key, secret_key


def build_s3_client():
    region = os.getenv("AWS_REGION", "eu-west-3")
    access_key, secret_key = resolve_aws_creds()
    if not access_key or not secret_key:
        raise ValueError("Missing AWS credentials for S3 upload")
    return boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def build_consumer() -> KafkaConsumer:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_VELIB_TOPIC", "velib-paris")
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="urban-pulse-velib-consumer",
        consumer_timeout_ms=15000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )


def main() -> None:
    load_dotenv()

    bucket = os.getenv("S3_BRONZE_BUCKET", "")
    if not bucket:
        raise ValueError("Missing S3_BRONZE_BUCKET")

    prefix = "bronze/velib/stream"
    consumer = build_consumer()
    s3 = build_s3_client()

    got_message = False
    for msg in consumer:
        got_message = True
        payload = msg.value
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        key = f"{prefix}/velib_stream_{ts}.json"
        body = io.BytesIO(json.dumps(payload).encode("utf-8"))
        s3.upload_fileobj(body, bucket, key)
        print(f"Uploaded stream message to s3://{bucket}/{key}")

    if not got_message:
        print("No Kafka message received in the last 15s. Keep producer running and relaunch consumer.")


if __name__ == "__main__":
    main()
