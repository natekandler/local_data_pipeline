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
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", os.path.join(os.getcwd(), "data", "raw.duckdb"))
# When set, use MotherDuck instead of local DuckDB file
MOTHERDUCK_DB = os.environ.get("MOTHERDUCK_DB")  # e.g., "waves". Requires MOTHERDUCK_TOKEN in env

LOCATIONS = {"Tamarack": {33.1505, -117.3483}, "Turnarounds": {33.1200, -117.3274}, "Oside_pier": {33.1934, -117.3860}}

def fetch_wave_data(latitude, longitude) -> Dict[str, Any]:
    """Fetch wave data from Open Meteo Marine API."""
    params = {
        "latitude": latitude,
        "longitude": longitude,
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

def _connect_duckdb():
    if MOTHERDUCK_DB:
        # MOTHERDUCK_TOKEN should be present in the environment for auth
        return duckdb.connect(f"md:{MOTHERDUCK_DB}")
    # Fallback to local DuckDB file
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    return duckdb.connect(DUCKDB_PATH)


def fetch_and_write_data(context: dg.AssetExecutionContext, latitude, longitude, location) -> dg.MaterializeResult:
    context.log.info(
        f"Fetching wave data for coordinates: {latitude}, {longitude} and writing to DuckDB at {DUCKDB_PATH}"
    )

    raw = fetch_wave_data(latitude, longitude)

    # Single-row record with raw payload
    now_ts = pd.Timestamp.now(tz="UTC")
    json_payload = json.dumps(raw)

    # Append to DuckDB table raw.open_meteo
    con = _connect_duckdb()
    try:
        # Use a schema that does not conflict with the database name
        con.execute("CREATE SCHEMA IF NOT EXISTS open_meteo")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS open_meteo.swell_data (
                timestamp TIMESTAMP,
                location TEXT,
                data TEXT
            )
            """
        )
        # Insert single row; let DuckDB parse ISO timestamp string into TIMESTAMP
        con.execute(
            "INSERT INTO open_meteo.swell_data (timestamp, location, data) VALUES (?, ?, ?)",
            [now_ts.isoformat(), location, json_payload],
        )
    finally:
        con.close()

    target = f"md:{MOTHERDUCK_DB}" if MOTHERDUCK_DB else DUCKDB_PATH
    context.log.info(f"Wrote 1 raw record for location '{location}' to DuckDB at {target}")

    return dg.MaterializeResult(
        metadata={
            "rows": 1,
            "location": LOCATION_NAME,
            "timestamp": now_ts.isoformat(),
            "duckdb_path": target,
            "table": "open_meteo.swell_data",
        }
    )


@dg.asset(
    description="Raw wave data from Open Meteo Marine API stored as Delta Lake on S3",
    group_name="wave_data",
)
def open_meteo(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Fetch wave data and write a single-row raw record to Delta on S3.

    Columns: timestamp (UTC), location (string), data (JSON string)
    Partitioned by: location
    """
    # Ensure local directory exists only when not using MotherDuck
    if not MOTHERDUCK_DB:
        os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    
    for location in LOCATIONS:
        lat, lon = LOCATIONS[location]
        fetch_and_write_data(context, lat, lon, location)
