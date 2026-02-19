from dagster import ScheduleDefinition
from src.jobs.school_geolocation_api import (
    school_geolocation_api__get_monogolia_school_updates,
)

school_geolocation_api__get_monogolia_school_updates_schedule = ScheduleDefinition(
    job=school_geolocation_api__get_monogolia_school_updates,
    cron_schedule="30 0 * * *",  # daily at 12.30AM
)
