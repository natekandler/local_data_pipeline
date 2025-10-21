import os

import dagster as dg
from dagster_dbt import DbtCliResource
try:
    # Newer versions export from package root
    from dagster_dbt import load_assets_from_dbt_project  # type: ignore
except ImportError:  # Fallback for versions that keep it under asset_defs
    from dagster_dbt.asset_defs import load_assets_from_dbt_project  # type: ignore


DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", os.path.join(os.getcwd(), "dbt"))
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", os.path.join(os.getcwd(), "dbt"))

# Cron and timezone can be overridden via environment variables
DBT_NIGHTLY_CRON = os.environ.get("DBT_NIGHTLY_CRON", "0 1 * * *")  # every day at 01:00
DBT_SCHEDULE_TZ = os.environ.get("DBT_SCHEDULE_TZ", os.environ.get("SCHEDULE_TZ", "UTC"))


# Define dbt assets from the dbt project
dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
    key_prefix=["dbt"],
    group_name="dbt",
)


# Resource to run dbt CLI commands (used by the dbt assets)
resources = {
    "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROFILES_DIR),
}


# Job to materialize only dbt assets
dbt_assets_job = dg.define_asset_job(
    name="dbt_assets",
    selection=dg.AssetSelection.groups("dbt"),
    description="Materialize dbt models defined in the dbt project.",
)


# Nightly schedule for dbt assets only
dbt_nightly_schedule = dg.ScheduleDefinition(
    job=dbt_assets_job,
    cron_schedule=DBT_NIGHTLY_CRON,
    execution_timezone=DBT_SCHEDULE_TZ,
    name="dbt_nightly_schedule",
    description="Runs dbt assets nightly.",
)


