from models.qos import SchoolList
from sqlalchemy import select

from dagster import RunConfig, RunRequest, ScheduleEvaluationContext, schedule
from src.jobs.qos import qos_school_list__automated_data_checks_job
from src.schemas.qos import SchoolListConfig
from src.utils.db import get_db_context


# one schedule for each freq of ingestion
@schedule(job=qos_school_list__automated_data_checks_job, cron_schedule="*/15 * * * *")
def qos_school_list__schedule(context: ScheduleEvaluationContext):
    # TODO: Use partitions instead
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%dT%H:%M")

    with get_db_context() as session:
        school_list_apis = session.scalars(select(SchoolList).where(SchoolList.enabled))

        for api in school_list_apis:
            config = SchoolListConfig.from_orm(api)

            yield RunRequest(
                run_key=f"{config.name}_{scheduled_date}",
                run_config=RunConfig(
                    ops={
                        "qos_school_list_raw": config,
                        "qos_school_list_bronze": config,
                        "qos_school_list_data_quality_results": config,
                        "qos_school_list_dq_passed_rows": config,
                        "qos_school_list_dq_failed_rows": config,
                        "qos_school_list_staging": config,
                    }
                ),
            )
