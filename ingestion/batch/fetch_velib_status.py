"""Fetches Velib station status and writes a timestamped JSON snapshot."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

VELIB_STATUS_URL = (
    "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/"
    "station_status.json"
)


def fetch_velib_status(url: str = VELIB_STATUS_URL) -> dict:
    req = Request(url, headers={"User-Agent": "urban-pulse-paris/1.0"})
    with urlopen(req, timeout=30) as response:
        payload = response.read().decode("utf-8")
    data = json.loads(payload)
    data["_metadata"] = {
        "source": url,
        "collected_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    return data


def write_snapshot(data: dict, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_file = output_dir / f"velib_status_{ts}.json"
    output_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_file


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Velib station status JSON")
    parser.add_argument(
        "--output-dir",
        default="data/raw/velib",
        help="Directory where snapshot files are saved",
    )
    args = parser.parse_args()

    data = fetch_velib_status()
    output_file = write_snapshot(data, Path(args.output_dir))
    print(f"Snapshot written: {output_file}")


if __name__ == "__main__":
    main()
