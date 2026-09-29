from src.jobs.qos_isizwe import isizwe_qos_job

from dagster import build_schedule_from_partitioned_job

isizwe_qos_schedule = build_schedule_from_partitioned_job(
    isizwe_qos_job, cron_schedule="40 2 * * *"
)
