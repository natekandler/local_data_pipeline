import os
import json
from typing import Any, Dict

import dagster as dg
import pandas as pd
import requests
import duckdb


WAVE_API_URL = "https://marine-api.open-meteo.com/v1/marine"
LATITUDE = 33.1505
LONGITUDE = -117.3483
LOCATION_NAME = os.environ.get("LOCATION_NAME", "Tamarack")
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", os.path.join(os.getcwd(), "data", "waves.duckdb"))


def fetch_wave_data() -> Dict[str, Any]:
    """Fetch wave data from Open Meteo Marine API."""
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "hourly": ",".join(
            [
                "wave_height",
                "wave_direction",
                "wind_wave_direction",
                "swell_wave_height",
                "swell_wave_direction",
                "swell_wave_period",
            ]
        ),
        "timezone": "auto",
    }

    response = requests.get(WAVE_API_URL, params=params, timeout=60)
    response.raise_for_status()
    return response.json()


# Note: We now store raw payloads as single rows with (timestamp, location, data)


@dg.asset(
    description="Raw wave data from Open Meteo Marine API stored as Delta Lake on S3",
    group_name="wave_data",
)
def open_meteo(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Fetch wave data and write a single-row raw record to Delta on S3.

    Columns: timestamp (UTC), location (string), data (JSON string)
    Partitioned by: location
    """
    # Ensure DuckDB directory exists
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)

    context.log.info(
        f"Fetching wave data for coordinates: {LATITUDE}, {LONGITUDE} and writing to DuckDB at {DUCKDB_PATH}"
    )

    raw = fetch_wave_data()

    # Single-row record with raw payload
    now_ts = pd.Timestamp.now(tz="UTC")
    json_payload = json.dumps(raw)

    # Append to DuckDB table raw.open_meteo
    con = duckdb.connect(DUCKDB_PATH)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS raw.open_meteo (
                timestamp TIMESTAMP,
                location TEXT,
                data TEXT
            )
            """
        )
        # Insert single row; let DuckDB parse ISO timestamp string into TIMESTAMP
        con.execute(
            "INSERT INTO raw.open_meteo (timestamp, location, data) VALUES (?, ?, ?)",
            [now_ts.isoformat(), LOCATION_NAME, json_payload],
        )
    finally:
        con.close()

    context.log.info(
        f"Wrote 1 raw record for location '{LOCATION_NAME}' to DuckDB at {DUCKDB_PATH}"
    )

    return dg.MaterializeResult(
        metadata={
            "rows": 1,
            "location": LOCATION_NAME,
            "timestamp": now_ts.isoformat(),
            "duckdb_path": DUCKDB_PATH,
            "table": "raw.open_meteo",
        }
    )
