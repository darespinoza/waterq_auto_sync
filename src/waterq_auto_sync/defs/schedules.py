import dagster as dg
from .jobs import (
    etapa_to_ierse_bmwp_job,
)

# Campbell daily partition Schedule
etapa_to_ierse_bmwp_schedule = dg.build_schedule_from_partitioned_job(
    job=etapa_to_ierse_bmwp_job,
    hour_of_day=0,
)


etapa_to_ierse_bmwp_schedule = dg.ScheduleDefinition(
    job=etapa_to_ierse_bmwp_job,
    cron_schedule="0 0 1 * *",
    execution_timezone="America/Guayaquil",
)

@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        schedules=[
            etapa_to_ierse_bmwp_schedule,
        ]
    )