import dagster as dg
from .jobs import (
    etapa_to_ierse_bmwp_job,
    etapa_to_ierse_wqi_job,
)

etapa_to_ierse_bmwp_schedule = dg.ScheduleDefinition(
    job=etapa_to_ierse_bmwp_job,
    cron_schedule="0 0 1 * *",
    execution_timezone="America/Guayaquil",
)

etapa_to_ierse_wqi_schedule = dg.ScheduleDefinition(
    job=etapa_to_ierse_wqi_job,
    cron_schedule="0 2 1 * *",
    execution_timezone="America/Guayaquil",
)

@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        schedules=[
            etapa_to_ierse_bmwp_schedule,
            etapa_to_ierse_wqi_schedule,
        ]
    )