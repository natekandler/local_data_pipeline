from pathlib import Path

from dagster import Definitions, load_from_defs_folder
from .defs import resources as defs_resources


def _build_defs() -> Definitions:
    loaded = load_from_defs_folder(path_within_project=Path(__file__).parent)
    return Definitions(
        assets=loaded.assets,
        schedules=loaded.schedules,
        sensors=loaded.sensors,
        jobs=loaded.jobs,
        resources=defs_resources,
    )

# Single module-level Definitions object for discovery
defs = _build_defs()
