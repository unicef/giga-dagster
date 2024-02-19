from dagster import RunConfig, RunRequest, ScheduleEvaluationContext, schedule
from src.jobs import qos__school_list_job


def generate_qos__school_list_schedule(api_name: str, config):
    @schedule(job=qos__school_list_job, cron_schedule="*/15 * * * *")
    def qos__school_list_schedule(context: ScheduleEvaluationContext):
        scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")

        yield RunRequest(
            run_key=f"{api_name}_{scheduled_date}",
            run_config=RunConfig(
                ops={
                    "list_raw": config,
                    "list_bronze": config,
                    "list_data_quality_results": config,
                    "list_dq_passed_rows": config,
                    "list_dq_failed_rows": config,
                    "list_staging": config,
                }
            ),
        )

    return qos__school_list_schedule
