import os
from pathlib import Path
import dagster as dg
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator


# --- Environment and paths ---
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", os.path.join(os.getcwd(), "dbt"))
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", os.path.join(os.getcwd(), "dbt"))
DBT_NIGHTLY_CRON = os.environ.get("DBT_NIGHTLY_CRON", "0 1 * * *")  # daily at 1am UTC
DBT_SCHEDULE_TZ = os.environ.get("DBT_SCHEDULE_TZ", os.environ.get("SCHEDULE_TZ", "UTC"))

MANIFEST_PATH = Path(DBT_PROJECT_DIR) / "target" / "manifest.json"


# --- Optional: Custom translator to add a key prefix ---
class PrefixedTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        # Add a "dbt" prefix to each asset key
        asset_key = super().get_asset_key(dbt_resource_props)
        return asset_key.with_prefix(["dbt"])


# --- Define dbt assets using modern decorator ---
@dbt_assets(manifest=MANIFEST_PATH, dagster_dbt_translator=PrefixedTranslator())
def dbt_project_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# --- Define the dbt CLI resource ---
resources = {
    "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROFILES_DIR),
}


# --- Define the job and schedule ---
dbt_assets_job = dg.define_asset_job(
    name="dbt_assets",
    selection=dg.AssetSelection.all(),
    description="Materialize dbt models defined in the dbt project.",
)

dbt_nightly_schedule = dg.ScheduleDefinition(
    job=dbt_assets_job,
    cron_schedule=DBT_NIGHTLY_CRON,
    execution_timezone=DBT_SCHEDULE_TZ,
    name="dbt_nightly_schedule",
    description="Runs dbt assets nightly.",
)
