from enum import Enum

from models.qos_apis import SchoolList
from sqlalchemy import select

from dagster import (
    RunConfig,
    RunRequest,
    ScheduleEvaluationContext,
    SkipReason,
    schedule,
)
from src.constants import DataTier, constants
from src.jobs.qos import qos_school_list__automated_data_checks_job
from src.schemas.qos import SchoolListConfig
from src.settings import settings
from src.utils.db import get_db_context
from src.utils.op_config import OpDestinationMapping, generate_run_ops

DATASET_TYPE = "school-list"
DOMAIN = "qos"


@schedule(job=qos_school_list__automated_data_checks_job, cron_schedule="*/2 * * * *")
def qos_school_list__schedule(context: ScheduleEvaluationContext):
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")

    with get_db_context() as session:
        school_list_apis = session.execute(select(SchoolList).where(SchoolList.enabled))

        count = 0
        for api in school_list_apis.mappings():
            row_data = SchoolListConfig.from_orm(
                {k: v.value if isinstance(v, Enum) else v for k, v in api.items()}
            )
            context.log.info(f"configzzz: {row_data.name}")

            country_code = "BRA"
            metastore_schema = "school_geolocation"
            stem = f"{row_data.name}_{scheduled_date}"

            ops_destination_mapping = {
                "qos_school_list_raw": OpDestinationMapping(
                    source_filepath="",
                    destination_filepath=f"{constants.raw_folder}/{DOMAIN}/{DATASET_TYPE}/{country_code}/{stem}.csv",
                    metastore_schema=metastore_schema,
                    tier=DataTier.RAW,
                ),
                "qos_school_list_bronze": OpDestinationMapping(
                    source_filepath=f"{constants.raw_folder}/{DOMAIN}/{DATASET_TYPE}/{country_code}/{stem}.csv",
                    destination_filepath=f"{constants.bronze_folder}/{DOMAIN}/{DATASET_TYPE}/{country_code}/{stem}.csv",
                    metastore_schema=metastore_schema,
                    tier=DataTier.BRONZE,
                ),
                "qos_school_list_data_quality_results": OpDestinationMapping(
                    source_filepath=f"{constants.bronze_folder}/{DOMAIN}/{DATASET_TYPE}/{country_code}/{stem}.csv",
                    destination_filepath=f"{constants.dq_results_folder}/{DOMAIN}/{DATASET_TYPE}/dq-overall/{country_code}/{stem}.csv",
                    metastore_schema=metastore_schema,
                    tier=DataTier.DATA_QUALITY_CHECKS,
                ),
                "qos_school_list_data_quality_results_summary": OpDestinationMapping(
                    source_filepath=f"{constants.dq_results_folder}/{DOMAIN}/{DATASET_TYPE}/dq-overall/{country_code}/{stem}.csv",
                    destination_filepath=f"{constants.dq_results_folder}/{DOMAIN}/{DATASET_TYPE}/dq-summary/{country_code}/{stem}.json",
                    metastore_schema=metastore_schema,
                    tier=DataTier.DATA_QUALITY_CHECKS,
                ),
                "qos_school_list_dq_passed_rows": OpDestinationMapping(
                    source_filepath=f"{constants.dq_results_folder}/{DOMAIN}/{DATASET_TYPE}/dq-overall/{country_code}/{stem}.csv",
                    destination_filepath=f"{constants.dq_results_folder}/{DOMAIN}/{DATASET_TYPE}/dq-passed-rows/{country_code}/{stem}.csv",
                    metastore_schema=metastore_schema,
                    tier=DataTier.DATA_QUALITY_CHECKS,
                ),
                "qos_school_list_dq_failed_rows": OpDestinationMapping(
                    source_filepath=f"{constants.dq_results_folder}/{DOMAIN}/{DATASET_TYPE}/dq-overall/{country_code}/{stem}.csv",
                    destination_filepath=f"{constants.dq_results_folder}/{DOMAIN}/{DATASET_TYPE}/dq-failed-rows/{country_code}/{stem}.csv",
                    metastore_schema=metastore_schema,
                    tier=DataTier.DATA_QUALITY_CHECKS,
                ),
                "qos_school_list_staging": OpDestinationMapping(
                    source_filepath=f"{constants.dq_results_folder}/{DOMAIN}/{DATASET_TYPE}/dq-passed-rows/{country_code}/{stem}.csv",
                    destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/school_geolocation_staging.db/{country_code.lower()}",
                    metastore_schema=metastore_schema,
                    tier=DataTier.STAGING,
                ),
            }

            run_ops = generate_run_ops(
                ops_destination_mapping,
                dataset_type=DATASET_TYPE,
                metadata={},
                file_size_bytes=0,
                domain=DOMAIN,
                dq_target_filepath=f"{constants.bronze_folder}/{DOMAIN}/{DATASET_TYPE}/{country_code}/{stem}.csv",
                country_code=country_code,
                database_data=row_data,
            )

            context.log.info("\n>>>>>>>>>>>>>>>\n")
            context.log.info(f"RUNOPS: {run_ops}")
            context.log.info(
                f"RUNDATA: {row_data.name}, {scheduled_date}, {country_code}"
            )

            yield RunRequest(
                run_key=f"{row_data.name}_{scheduled_date}",
                run_config=RunConfig(ops=run_ops),
                tags={"country": country_code},
            )

            count += 1

    if count == 0:
        yield SkipReason("No enabled school list APIs detected in PostgreSQL database")
