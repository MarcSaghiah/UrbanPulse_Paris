import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener

from airflow import DAG
from airflow.operators.python import PythonOperator

VELIB_STATUS_URL = (
    "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/"
    "station_status.json"
)

# Store local snapshots in the project data folder mounted into Airflow.
OUTPUT_DIR = Path("/opt/airflow/data/raw/velib")


def fetch_and_save_velib_snapshot() -> None:
    # Some Docker environments inject proxy variables that are unreachable from
    # the Airflow container. Force direct outbound HTTPS calls for this source.
    opener = build_opener(ProxyHandler({}))

    payload = None
    for attempt in range(1, 4):
        try:
            req = Request(VELIB_STATUS_URL, headers={"User-Agent": "urban-pulse-paris/1.0"})
            with opener.open(req, timeout=90) as response:
                payload = response.read().decode("utf-8")
            break
        except (HTTPError, URLError, TimeoutError) as exc:
            if attempt == 3:
                raise RuntimeError(f"Velib API request failed after 3 attempts: {exc}") from exc
            sleep_seconds = attempt * 5
            print(f"Attempt {attempt}/3 failed: {exc}. Retrying in {sleep_seconds}s...")
            time.sleep(sleep_seconds)

    if payload is None:
        raise RuntimeError("Velib API request returned no payload")

    data = json.loads(payload)
    data["_metadata"] = {
        "source": VELIB_STATUS_URL,
        "collected_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_file = OUTPUT_DIR / f"velib_status_{ts}.json"
    output_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    station_count = len(data.get("data", {}).get("stations", []))
    print(f"Saved {output_file}")
    print(f"Station count: {station_count}")


default_args = {
    "owner": "marcsag",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="dag_velib_ingestion",
    default_args=default_args,
    description="Fetches Velib status and stores timestamped snapshots",
    start_date=datetime(2024, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    tags=["ingestion", "velib", "paris"],
) as dag:
    ingest = PythonOperator(
        task_id="fetch_and_save_velib_snapshot",
        python_callable=fetch_and_save_velib_snapshot,
    )

    ingest
