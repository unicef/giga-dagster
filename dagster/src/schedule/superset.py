from dagster import ScheduleDefinition

from ..jobs.incremental import refresh_incremental_table
from ..jobs.superset import refresh_table
from ..jobs.incremental import refresh_incremental_table

superset_schedule = ScheduleDefinition(
    job=refresh_table,
    cron_schedule="15 3 * * *",
)

superset_incremental_schedule = ScheduleDefinition(
    job=refresh_incremental_table,
    cron_schedule="15 * * * *",
)
