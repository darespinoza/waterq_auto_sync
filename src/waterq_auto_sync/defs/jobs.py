import dagster as dg
from .assets import (
    pg_waterq_stations,
    bwmp_data_req
)

# Postgres to Minio job
etapa_to_ierse_job = dg.define_asset_job(
    name="etapa_to_ierse_job",
    selection=[pg_waterq_stations, bwmp_data_req],
)


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        jobs=[
            etapa_to_ierse_job,
        ]
    )