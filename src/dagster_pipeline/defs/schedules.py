import os

import dagster as dg


NIGHTLY_CRON = os.environ.get("NIGHTLY_CRON", "0 0 * * *")  # every day at 00:00
SCHEDULE_TZ = os.environ.get("SCHEDULE_TZ", "UTC")


nightly_assets_job = dg.define_asset_job(
    name="nightly_assets",
    selection=dg.AssetSelection.all(),
    description="Materialize all assets nightly.",
)


nightly_assets_schedule = dg.ScheduleDefinition(
    job=nightly_assets_job,
    cron_schedule=NIGHTLY_CRON,
    execution_timezone=SCHEDULE_TZ,
    name="nightly_assets_schedule",
    description="Runs nightly_assets job on a nightly cadence.",
)


