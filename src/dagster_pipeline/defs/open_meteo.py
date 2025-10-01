import os
from typing import Any, Dict, List

import dagster as dg
import pandas as pd
import requests
from deltalake import write_deltalake


WAVE_API_URL = "https://marine-api.open-meteo.com/v1/marine"
LATITUDE = 33.1505
LONGITUDE = -117.3483


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


def process_wave_data(data: Dict[str, Any]) -> pd.DataFrame:
    """Transform API response into a pandas DataFrame with a date partition column."""
    hourly = data["hourly"]
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(hourly["time"]),
            "wave_height": hourly["wave_height"],
            "wave_direction": hourly["wave_direction"],
            "wind_wave_direction": hourly["wind_wave_direction"],
            "swell_wave_height": hourly["swell_wave_height"],
            "swell_wave_direction": hourly["swell_wave_direction"],
            "swell_wave_period": hourly["swell_wave_period"],
        }
    )

    # Partition column as YYYY-MM-DD string for stable partitioning
    frame["dt"] = frame["timestamp"].dt.strftime("%Y-%m-%d")
    return frame


@dg.asset(
    description="Raw wave data from Open Meteo Marine API stored as Delta Lake on S3",
    group_name="wave_data",
)
def open_meteo(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Fetch wave data and append to a Delta Lake table on S3 partitioned by date."""
    bucket_name = os.environ.get("RAW_BUCKET", "dwh")
    base_prefix = os.environ.get("RAW_PREFIX", "raw/wave_data")
    s3_uri = f"s3://{bucket_name}/{base_prefix}"

    context.log.info(
        f"Fetching wave data for coordinates: {LATITUDE}, {LONGITUDE} and writing to {s3_uri}"
    )

    raw = fetch_wave_data()
    df = process_wave_data(raw)

    unique_dates: List[str] = sorted(df["dt"].unique().tolist())

    # Write or append to Delta table; will create the table if it doesn't exist
    # Credentials are read from environment: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, etc.
    write_deltalake(
        s3_uri,
        df,
        mode="append",
        partition_by=["dt"],
    )

    context.log.info(
        f"Wrote {len(df)} rows across {len(unique_dates)} date partitions to {s3_uri}"
    )

    return dg.MaterializeResult(
        metadata={
            "rows": len(df),
            "partitions": len(unique_dates),
            "dates": ",".join(unique_dates),
            "delta_table": s3_uri,
        }
    )
