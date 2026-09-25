from dagster import build_schedule_from_partitioned_job
from src.jobs.mlab_traceroutes import mlab_traceroutes__pull_measurements

mlab_traceroutes__pull_measurements_schedule = build_schedule_from_partitioned_job(
    mlab_traceroutes__pull_measurements,
    hour_of_day=4,
    minute_of_hour=45,
)
