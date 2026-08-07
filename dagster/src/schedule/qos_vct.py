from dagster import ScheduleDefinition
from src.jobs.qos_vct import (
    vct_combined_daily_job,
    vct_events_daily_job,
    vct_qos_60min_job,
)

vct_events_daily_schedule = ScheduleDefinition(
    job=vct_events_daily_job, cron_schedule="0 1 * * *"
)
vct_qos_60min_schedule = ScheduleDefinition(
    job=vct_qos_60min_job, cron_schedule="0 * * * *"
)
vct_combined_daily_schedule = ScheduleDefinition(
    job=vct_combined_daily_job, cron_schedule="0 2 * * *"
)
