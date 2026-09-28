from dagster import build_schedule_from_partitioned_job
from src.jobs.giga_meter import giga_meter__mlab_traceroutes_job

giga_meter__mlab_traceroutes_schedule = build_schedule_from_partitioned_job(
    giga_meter__mlab_traceroutes_job,
    hour_of_day=4,
    minute_of_hour=45,
)
