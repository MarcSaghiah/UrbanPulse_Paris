from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from urllib.request import Request, urlopen

from kafka import KafkaProducer
from dotenv import load_dotenv

VELIB_STATUS_URL = (
    "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/"
    "station_status.json"
)


def build_producer() -> KafkaProducer:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def fetch_payload() -> dict:
    req = Request(VELIB_STATUS_URL, headers={"User-Agent": "urban-pulse-paris/1.0"})
    with urlopen(req, timeout=60) as response:
        data = json.loads(response.read().decode("utf-8"))
    data["_metadata"] = {
        "source": VELIB_STATUS_URL,
        "collected_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    return data


def main() -> None:
    load_dotenv()

    topic = os.getenv("KAFKA_VELIB_TOPIC", "velib-paris")
    interval_seconds = int(os.getenv("VELIB_STREAM_INTERVAL_SECONDS", "60"))
    producer = build_producer()

    while True:
        payload = fetch_payload()
        producer.send(topic, payload)
        producer.flush()
        print(f"Produced 1 event to topic={topic}")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
