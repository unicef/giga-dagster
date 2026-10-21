from dagster import ScheduleDefinition

from ..jobs.maintenance import maintenance__optimize_incremental_tables_job

maintenance__optimize_incremental_tables_schedule = ScheduleDefinition(
    job=maintenance__optimize_incremental_tables_job,
    cron_schedule="45 0 * * *",
)
