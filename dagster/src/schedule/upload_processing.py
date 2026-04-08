from dagster import ScheduleDefinition
from src.jobs.upload_processing import upload_processing_job

upload_processing_schedule = ScheduleDefinition(
    job=upload_processing_job,
    cron_schedule="0 0 * * *",  # Run daily at 0 UTC
    execution_timezone="UTC",
)
