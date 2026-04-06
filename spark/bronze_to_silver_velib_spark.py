from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_timestamp


def main() -> None:
    bronze_path = os.getenv("SPARK_BRONZE_PATH", "s3a://urban-pulse-paris-bronze/bronze/velib/status/")
    silver_path = os.getenv("SPARK_SILVER_PATH", "s3a://urban-pulse-paris-silver/silver/velib/station_status_spark/")

    spark = (
        SparkSession.builder.appName("urban-pulse-bronze-to-silver-velib")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    raw = spark.read.json(bronze_path)
    stations = raw.select(col("data.stations").alias("stations"), col("_metadata.collected_at_utc").alias("collected_at_utc"))

    flattened = (
        stations
        .selectExpr("explode(stations) as station", "collected_at_utc")
        .select(
            col("station.station_id").alias("station_id"),
            col("station.is_installed").alias("is_installed"),
            col("station.is_renting").alias("is_renting"),
            col("station.is_returning").alias("is_returning"),
            col("station.num_bikes_available").alias("num_bikes_available"),
            col("station.num_docks_available").alias("num_docks_available"),
            to_timestamp(col("collected_at_utc")).alias("collected_at_ts"),
            current_timestamp().alias("processed_at_utc"),
        )
    )

    (
        flattened.write.mode("append").partitionBy("is_renting").parquet(silver_path)
    )

    spark.stop()


if __name__ == "__main__":
    main()
