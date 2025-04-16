from dagster import ScheduleDefinition

from ..jobs.superset import refresh_table

superset_schedule = ScheduleDefinition(
    job=refresh_table,
    cron_schedule="0 5 * * *",
)
