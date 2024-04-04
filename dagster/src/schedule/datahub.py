from dagster import ScheduleDefinition
from src.jobs.datahub import (
    datahub__ingest_coverage_notebooks_from_github_job,
    datahub__materialize_prerequisites_job,
    datahub__update_access_job,
)
from src.settings import settings

datahub_materialize_prerequisities_schedule = ScheduleDefinition(
    job=datahub__materialize_prerequisites_job,
    cron_schedule="0 0 1 1 *",  # yearly
    default_status=settings.DEFAULT_SCHEDULE_STATUS,
)

datahub_update_access_schedule = ScheduleDefinition(
    job=datahub__update_access_job,
    cron_schedule=settings.DEFAULT_SCHEDULE_CRON,
    default_status=settings.DEFAULT_SCHEDULE_STATUS,
)

datahub_ingest_coverage_notebooks_schedule = ScheduleDefinition(
    job=datahub__ingest_coverage_notebooks_from_github_job,
    cron_schedule="*0 0 1 * *",  # monthly
    default_status=settings.DEFAULT_SCHEDULE_STATUS,
)
