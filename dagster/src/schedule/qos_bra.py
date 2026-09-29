from dagster import ScheduleDefinition
from src.jobs.qos_bra import bra_qos_job, bra_qos_raw_republish_job

bra_qos_schedule = ScheduleDefinition(
    job=bra_qos_job, cron_schedule="10 3,7,11,15,19,23 * * *"
)
bra_qos_raw_republish_schedule = ScheduleDefinition(
    job=bra_qos_raw_republish_job, cron_schedule="45 1 * * *"
)
