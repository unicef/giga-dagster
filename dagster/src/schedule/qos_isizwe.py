from dagster import ScheduleDefinition
from src.jobs.qos_isizwe import isizwe_qos_job

isizwe_qos_schedule = ScheduleDefinition(job=isizwe_qos_job, cron_schedule="40 2 * * *")
