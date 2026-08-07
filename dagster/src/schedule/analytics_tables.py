from dagster import ScheduleDefinition

from ..jobs.analytics_tables import (
    analytics_tables_daily_job,
    analytics_tables_incremental_job,
)

analytics_tables_daily_schedule = ScheduleDefinition(
    job=analytics_tables_daily_job,
    cron_schedule="45 3 * * *",
)

analytics_tables_incremental_schedule = ScheduleDefinition(
    job=analytics_tables_incremental_job,
    cron_schedule="15 * * * *",
)
