from dagster import ScheduleDefinition
from src.jobs.qos_mawingu import mawingu_qos_job

mawingu_qos_schedule = ScheduleDefinition(job=mawingu_qos_job, cron_schedule="10 2 * * *")
