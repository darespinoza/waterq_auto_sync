import dagster as dg
from .assets import (
    pg_waterq_stations,
)
from .bmwp_assets import(
    mfqb_data_raw,
    mfqb_data_bronze,
    mfqb_data_silver,
)

# Postgres to Minio job
etapa_to_ierse_bmwp_job = dg.define_asset_job(
    name="etapa_to_ierse_bmwp_job",
    selection=[pg_waterq_stations, mfqb_data_raw, mfqb_data_bronze, mfqb_data_silver,],
)


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        jobs=[
            etapa_to_ierse_bmwp_job,
        ]
    )