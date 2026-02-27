import dagster as dg
from .assets import (
    pg_waterq_stations,
    mfqb_data_raw
)

# Postgres to Minio job
etapa_to_ierse_bmwp_job = dg.define_asset_job(
    name="etapa_to_ierse_bmwp_job",
    selection=[pg_waterq_stations, mfqb_data_raw],
)


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        jobs=[
            etapa_to_ierse_bmwp_job,
        ]
    )