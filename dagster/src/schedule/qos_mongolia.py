from src.jobs.qos_mongolia import mongolia_qos_gold_job, mongolia_qos_raw_json_job

from dagster import ScheduleDefinition, build_schedule_from_partitioned_job

# not partitioned - there's nothing to backfill, a missed 5-minute poll is just gone.
mongolia_qos_raw_json_schedule = ScheduleDefinition(
    job=mongolia_qos_raw_json_job, cron_schedule="4,9,14,19,24,29,34,39,44,49,54,59 * * * *"
)

mongolia_qos_gold_schedule = build_schedule_from_partitioned_job(
    mongolia_qos_gold_job, cron_schedule="55 15 * * *"
)
