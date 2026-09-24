from dagster import ScheduleDefinition
from src.jobs.mlab_traceroutes import mlab_traceroutes__pull_measurements

mlab_traceroutes__pull_measurements_schedule = ScheduleDefinition(
    job=mlab_traceroutes__pull_measurements,
    cron_schedule="45 4 * * *",  # daily at 03:45 UTC
    execution_timezone="UTC",
)
