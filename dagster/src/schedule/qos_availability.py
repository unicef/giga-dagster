from dagster import ScheduleDefinition

from src.jobs.qos_availability import generate_uptime

superset_schedule = ScheduleDefinition(
    job=generate_uptime,
    cron_schedule="10 * * * *",
)
