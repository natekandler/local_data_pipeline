import os
import json
from typing import Any, Dict, List

import dagster as dg
import pandas as pd
import requests
from deltalake import write_deltalake


WAVE_API_URL = "https://marine-api.open-meteo.com/v1/marine"
LATITUDE = 33.1505
LONGITUDE = -117.3483
LOCATION_NAME = os.environ.get("LOCATION_NAME", "Tamarack")


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
    bucket_name = os.environ.get("RAW_BUCKET", "dwh")
    base_prefix = os.environ.get("RAW_PREFIX", "raw/wave_data")
    s3_uri = f"s3://{bucket_name}/{base_prefix}"

    context.log.info(
        f"Fetching wave data for coordinates: {LATITUDE}, {LONGITUDE} and writing to {s3_uri}"
    )

    raw = fetch_wave_data()

    # Single-row record with raw payload
    now_ts = pd.Timestamp.now(tz="UTC")
    record = pd.DataFrame(
        [
            {
                "timestamp": now_ts,
                "location": LOCATION_NAME,
                "data": json.dumps(raw),
            }
        ]
    )

    # Write or append to Delta table; creates the table if it does not exist
    write_deltalake(
        s3_uri,
        record,
        mode="append",
        partition_by=["location"],
    )

    context.log.info(
        f"Wrote 1 raw record for location '{LOCATION_NAME}' to {s3_uri}"
    )

    return dg.MaterializeResult(
        metadata={
            "rows": 1,
            "location": LOCATION_NAME,
            "timestamp": now_ts.isoformat(),
            "delta_table": s3_uri,
        }
    )
