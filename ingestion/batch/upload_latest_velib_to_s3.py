from __future__ import annotations

import argparse
import os
from pathlib import Path

import boto3


def find_latest_snapshot(input_dir: Path) -> Path:
    files = sorted(input_dir.glob("velib_status_*.json"))
    if not files:
        raise FileNotFoundError(f"No Velib snapshot found in {input_dir}")
    return files[-1]


def resolve_aws_creds() -> tuple[str | None, str | None]:
    access_key = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_KEY")
    return access_key, secret_key


def upload_file_to_s3(file_path: Path, bucket: str, region: str, prefix: str) -> str:
    s3_key = f"{prefix.rstrip('/')}/{file_path.name}"
    access_key, secret_key = resolve_aws_creds()
    if not access_key or not secret_key:
        raise ValueError(
            "Missing AWS credentials. Set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
            "or AWS_ACCESS_KEY/AWS_SECRET_KEY in environment."
        )

    client = boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    client.upload_file(str(file_path), bucket, s3_key)
    return s3_key


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload latest Velib snapshot to S3 Bronze")
    parser.add_argument(
        "--input-dir",
        default="data/raw/velib",
        help="Directory containing local Velib JSON snapshots",
    )
    parser.add_argument(
        "--bucket",
        default=os.getenv("S3_BRONZE_BUCKET", ""),
        help="S3 bucket name for Bronze layer",
    )
    parser.add_argument(
        "--region",
        default=os.getenv("AWS_REGION", "eu-west-3"),
        help="AWS region",
    )
    parser.add_argument(
        "--prefix",
        default="bronze/velib/status",
        help="S3 key prefix",
    )
    args = parser.parse_args()

    if not args.bucket:
        raise ValueError("Missing bucket name. Set --bucket or S3_BRONZE_BUCKET env var.")

    latest_file = find_latest_snapshot(Path(args.input_dir))
    s3_key = upload_file_to_s3(latest_file, args.bucket, args.region, args.prefix)

    print(f"Uploaded {latest_file} to s3://{args.bucket}/{s3_key}")


if __name__ == "__main__":
    main()
