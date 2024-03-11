import pandas as pd
from pyspark import sql

from dagster import RunConfig, RunRequest, ScheduleEvaluationContext, schedule
from src.jobs.qos import qos_list__automated_data_checks_job
from src.utils.apis import QOSSchoolListAPIData


@schedule(job=qos_list__automated_data_checks_job, cron_schedule="5-59/15 * * * *")
def qos_list__schedule(context: ScheduleEvaluationContext):
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")

    school_list_apis = sql.execute(
        "SELECT * FROM school_list_apis WHERE enabled = True"
    ).tolist()  # something like this

    df = pd.DataFrame(school_list_apis)

    for i in range(len(df)):
        config: QOSSchoolListAPIData = df.loc[i, :].to_dict()

        yield RunRequest(
            run_key=f"{df.loc[i, "name"]}_{scheduled_date}",
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
