from datetime import datetime, timedelta
from enum import Enum

from models.qos_apis import SchoolList
from sqlalchemy import select

from dagster import RunConfig, RunRequest, SensorEvaluationContext, SkipReason, sensor
from src.constants import DataTier, constants
from src.jobs.qos import qos_school_list__automated_data_checks_job
from src.schemas.qos import SchoolListConfig
from src.settings import settings
from src.utils.db import get_db_context
from src.utils.op_config import OpDestinationMapping, generate_run_ops

DOMAIN = "qos"
DATASET_TYPE = "school-list"
DOMAIN_DATASET_TYPE = f"{DOMAIN}-{DATASET_TYPE}"


@sensor(
    job=qos_school_list__automated_data_checks_job,
    minimum_interval_seconds=int(timedelta(days=1).total_seconds())
    if settings.IN_PRODUCTION
    else 30,
)
def qos_school_list__new_apis_sensor(
    context: SensorEvaluationContext,
):
    scheduled_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with get_db_context() as session:
        school_list_apis = session.scalars(select(SchoolList).where(SchoolList.enabled))

        count = 0
        for api in school_list_apis:
            config = {}
            for key, value in api.__dict__.items():
                if isinstance(value, Enum):
                    config[key] = value.value
                elif isinstance(value, datetime):
                    config[key] = value.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    config[key] = value

            row_data = SchoolListConfig(**config)

            country_code = "RWA"
            metastore_schema = "school_geolocation"
            stem = f"{row_data.name}_{country_code}_{DOMAIN_DATASET_TYPE}_{scheduled_date.replace(' ', '_').replace(':', '').replace('-', '')}"

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
                database_data=row_data.json(),
            )

            formatted_date = (
                scheduled_date.replace(" ", "").replace("-", "_").replace(":", "_")
            )
            run_key = f"{row_data.name}_{formatted_date}"

            yield RunRequest(
                run_key=run_key,
                run_config=RunConfig(ops=run_ops),
                tags={"country": country_code},
            )

            count += 1

    if count == 0:
        yield SkipReason("No enabled school list APIs detected in PostgreSQL database")
