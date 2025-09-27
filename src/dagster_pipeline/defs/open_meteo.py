import dagster as dg


@dg.asset
def open_meteo(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...
