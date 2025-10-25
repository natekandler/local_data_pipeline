# Expose resources at the package level so the defs loader can pick them up
try:
    from .dbt_defs import resources as dbt_resources  # type: ignore
except Exception:
    dbt_resources = {}

resources = {}
resources.update(dbt_resources)
