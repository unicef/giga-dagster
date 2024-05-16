from datetime import UTC, datetime, timedelta
from enum import Enum

from croniter import croniter
from models.qos_apis import SchoolConnectivity, SchoolList
from sqlalchemy.orm import joinedload

from dagster import RunConfig, RunRequest, SensorEvaluationContext, SkipReason, sensor
from src.constants import DataTier, constants
from src.jobs.qos import (
    qos_school_connectivity__automated_data_checks_job,
    qos_school_list__automated_data_checks_job,
)
from src.schemas.qos import SchoolConnectivityConfig, SchoolListConfig
from src.settings import settings
from src.utils.db.primary import get_db_context
from src.utils.op_config import OpDestinationMapping, generate_run_ops

DOMAIN = "qos"


def generate_dagster_config(api_data: dict[str, str]) -> dict[str, str]:
    config = {}
    for key, value in api_data.items():
        if isinstance(value, Enum):
            config[key] = value.value
        elif isinstance(value, datetime):
            config[key] = value.strftime("%Y-%m-%d %H:%M:%S")
        else:
            config[key] = value

    return config


@sensor(
    job=qos_school_list__automated_data_checks_job,
    minimum_interval_seconds=int(timedelta(days=1).total_seconds())
    if settings.IN_PRODUCTION
    else 30,
)
def qos_school_list__new_apis_sensor(
    context: SensorEvaluationContext,
):
    DATASET_TYPE = "school-list"
    DOMAIN_DATASET_TYPE = f"{DOMAIN}-{DATASET_TYPE}"

    scheduled_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with get_db_context() as session:
        school_list_apis = session.query(SchoolList).filter(SchoolList.enabled).all()

        count = 0
        for api in school_list_apis:
            config = generate_dagster_config(api.__dict__)

            row_data = SchoolListConfig(**config)

            country_code = row_data.country
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


@sensor(
    job=qos_school_connectivity__automated_data_checks_job,
    minimum_interval_seconds=int(timedelta(minutes=10).total_seconds())
    if settings.IN_PRODUCTION
    else int(timedelta(minutes=5).total_seconds()),
)
def qos_school_connectivity__new_apis_sensor(context: SensorEvaluationContext):
    DATASET_TYPE = "school-connectivity"
    DOMAIN_DATASET_TYPE = f"{DOMAIN}-{DATASET_TYPE}"

    scheduled_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with get_db_context() as session:
        school_connectivity_apis = (
            session.query(SchoolConnectivity)
            .filter(SchoolConnectivity.enabled)
            .options(joinedload(SchoolConnectivity.school_list))
        )

    count = 0
    for api in school_connectivity_apis:
        if api.date_last_ingested:
            next_execution_date = croniter(
                api.ingestion_frequency, api.date_last_ingested
            ).get_next(datetime)
            context.log.info(
                f">>> next exec!!last: {api.date_last_ingested}, {next_execution_date}, {datetime.now(UTC)}"
            )
            context.log.info(
                f">>> next execyes?!!: {api.ingestion_frequency}, {next_execution_date > datetime.now(UTC)}"
            )
            if next_execution_date > datetime.now(UTC):
                continue

        school_connectivity_config = generate_dagster_config(api.__dict__)
        school_list_config = generate_dagster_config(api.school_list.__dict__)
        school_connectivity_config["school_list"] = school_list_config

        row_data = SchoolConnectivityConfig(**school_connectivity_config)

        country_code = row_data.school_list.country
        metastore_schema = "qos"
        stem = f"{row_data.school_list.name.replace(' ', '-')}_{country_code}_{DOMAIN_DATASET_TYPE}_{scheduled_date.replace(' ', '_').replace(':', '').replace('-', '')}"

        ops_destination_mapping = {
            "qos_school_connectivity_raw": OpDestinationMapping(
                source_filepath="",
                destination_filepath=f"{constants.raw_folder}/{DOMAIN}/{DATASET_TYPE}/{country_code}/{stem}.csv",
                metastore_schema=metastore_schema,
                tier=DataTier.RAW,
            ),
            "qos_school_connectivity_bronze": OpDestinationMapping(
                source_filepath=f"{constants.raw_folder}/{DOMAIN}/{DATASET_TYPE}/{country_code}/{stem}.csv",
                destination_filepath=f"{constants.bronze_folder}/{DOMAIN}/{DATASET_TYPE}/{country_code}/{stem}.csv",
                metastore_schema=metastore_schema,
                tier=DataTier.RAW,
            ),
            "qos_school_connectivity_data_quality_results": OpDestinationMapping(
                source_filepath=f"{constants.bronze_folder}/{DOMAIN}/{DATASET_TYPE}/{country_code}/{stem}.csv",
                destination_filepath=f"{constants.dq_results_folder}/{DOMAIN}/{DATASET_TYPE}/dq-overall/{country_code}/{stem}.csv",
                metastore_schema=metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "qos_school_connectivity_data_quality_results_summary": OpDestinationMapping(
                source_filepath=f"{constants.dq_results_folder}/{DOMAIN}/{DATASET_TYPE}/dq-overall/{country_code}/{stem}.csv",
                destination_filepath=f"{constants.dq_results_folder}/{DOMAIN}/{DATASET_TYPE}/dq-summary/{country_code}/{stem}.json",
                metastore_schema=metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "qos_school_connectivity_dq_passed_rows": OpDestinationMapping(
                source_filepath=f"{constants.dq_results_folder}/{DOMAIN}/{DATASET_TYPE}/dq-overall/{country_code}/{stem}.csv",
                destination_filepath=f"{constants.dq_results_folder}/{DOMAIN}/{DATASET_TYPE}/dq-passed-rows/{country_code}/{stem}.csv",
                metastore_schema=metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "qos_school_connectivity_dq_failed_rows": OpDestinationMapping(
                source_filepath=f"{constants.dq_results_folder}/{DOMAIN}/{DATASET_TYPE}/dq-overall/{country_code}/{stem}.csv",
                destination_filepath=f"{constants.dq_results_folder}/{DOMAIN}/{DATASET_TYPE}/dq-failed-rows/{country_code}/{stem}.csv",
                metastore_schema=metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "qos_school_connectivity_gold": OpDestinationMapping(
                source_filepath=f"{constants.dq_results_folder}/{DOMAIN}/{DATASET_TYPE}/dq-passed-rows/{country_code}/{stem}.csv",
                destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/{metastore_schema}_gold.db/{country_code.lower()}",
                metastore_schema=metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
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
        run_key = f"{row_data.school_list.name}_{formatted_date}"

        yield RunRequest(
            run_key=run_key,
            run_config=RunConfig(ops=run_ops),
            tags={"country": country_code},
        )

        count += 1

    if count == 0:
        yield SkipReason(
            "No enabled school connectivity APIs detected in PostgreSQL database"
        )
