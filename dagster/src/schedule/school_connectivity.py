from dagster import ScheduleDefinition
from src.jobs.school_connectivity import school_connectivity__new_realtime_schools_job

school_connectivity__new_realtime_schools_schedule = ScheduleDefinition(
    job=school_connectivity__new_realtime_schools_job,
    cron_schedule="0 0 * * *",  # daily at midnight
)
