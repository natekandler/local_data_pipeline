import os
import re
from typing import Any, Dict, Tuple

import dagster as dg
import pandas as pd
import requests


WAVE_API_URL = "https://marine-api.open-meteo.com/v1/marine"
S3_BASE_PATH = os.environ.get(
    "SWELL_DATA_S3_PATH", "s3://nk-data-lake/raw-data/swell_data"
)

LOCATIONS: Dict[str, Tuple[float, float]] = {
    "Tamarack": (33.1505, -117.3483),
    "Turnarounds": (33.1200, -117.3274),
    "Oside_pier": (33.1934, -117.3860),
}


def fetch_wave_data(latitude: float, longitude: float) -> Dict[str, Any]:
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


def _slugify_location(location: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", location.lower()).strip("_")
    return slug or "location"


def _normalize_hourly_payload(
    raw: Dict[str, Any], location: str, retrieved_at: pd.Timestamp
) -> pd.DataFrame:
    hourly = raw.get("hourly", {})
    if not hourly:
        return pd.DataFrame()

    df = pd.DataFrame(hourly)
    if df.empty or "time" not in df.columns:
        return pd.DataFrame()

    df["timestamp"] = pd.to_datetime(df["time"], utc=True)
    df = df.drop(columns=["time"])
    df.insert(0, "timestamp", df.pop("timestamp"))
    df["location"] = location
    df["retrieved_at"] = retrieved_at

    return df


def _write_location_dataset(
    df: pd.DataFrame, location_slug: str, run_ts: pd.Timestamp
) -> str:
    partition_date = run_ts.strftime("%Y-%m-%d")
    partition_hour = run_ts.strftime("%H")
    file_name = f"{location_slug}_{run_ts.strftime('%Y%m%dT%H%M%S')}.parquet"

    target_path = (
        f"{S3_BASE_PATH}/location={location_slug}/date={partition_date}/hour={partition_hour}/{file_name}"
    )

    df.to_parquet(target_path, index=False)
    return target_path


@dg.asset(
    description=(
        "Hourly swell and wave observations from Open Meteo Marine API written to "
        "partitioned Parquet files in S3."
    ),
    group_name="wave_data",
)
def swell_data_s3(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    run_ts = pd.Timestamp.now(tz="UTC")
    storage_metadata = {
        "run_timestamp": run_ts.isoformat(),
        "s3_base_path": S3_BASE_PATH,
    }
    total_rows = 0

    for location, (latitude, longitude) in LOCATIONS.items():
        location_slug = _slugify_location(location)
        context.log.info(
            "Fetching wave data for %s (%s, %s)", location, latitude, longitude
        )

        raw = fetch_wave_data(latitude, longitude)
        df = _normalize_hourly_payload(raw, location, run_ts)

        if df.empty:
            context.log.warning(
                "Received no hourly data for location %s; skipping write.", location
            )
            continue

        target_path = _write_location_dataset(df, location_slug, run_ts)
        rows = len(df.index)
        total_rows += rows

        storage_metadata[f"{location_slug}_rows"] = rows
        storage_metadata[f"{location_slug}_path"] = dg.MetadataValue.path(target_path)
        context.log.info(
            "Wrote %s rows for location %s to %s", rows, location, target_path
        )

    storage_metadata["total_rows"] = total_rows

    if total_rows == 0:
        context.log.warning(
            "No data was written for any location during this run of swell_data_s3."
        )

    return dg.MaterializeResult(metadata=storage_metadata)

