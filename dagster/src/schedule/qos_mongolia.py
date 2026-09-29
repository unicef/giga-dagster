from dagster import ScheduleDefinition
from src.jobs.qos_mongolia import mongolia_qos_gold_job, mongolia_qos_raw_json_job

mongolia_qos_raw_json_schedule = ScheduleDefinition(
    job=mongolia_qos_raw_json_job, cron_schedule="4,9,14,19,24,29,34,39,44,49,54,59 * * * *"
)
mongolia_qos_gold_schedule = ScheduleDefinition(
    job=mongolia_qos_gold_job, cron_schedule="55 15 * * *"
)
