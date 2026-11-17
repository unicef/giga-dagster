from dagster import build_schedule_from_partitioned_job
from src.jobs.qos_isizwe import isizwe_qos_job

isizwe_qos_schedule = build_schedule_from_partitioned_job(
    isizwe_qos_job, hour_of_day=2, minute_of_hour=40
)
