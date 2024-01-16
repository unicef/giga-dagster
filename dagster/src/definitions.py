from dagster_ge.factory import GEContextResource

from dagster import Definitions, load_assets_from_package_module
from src.assets import assets, datahub_assets, qos
from src.jobs import (
    datahub__create_domains_job,
    datahub__create_tags_job,
    datahub__ingest_azure_ad_users_groups_job,
    datahub__update_policies_job,
    qos__convert_csv_to_deltatable_job,
    school_master__automated_data_checks_job,
    school_master__convert_gold_csv_to_deltatable_job,
    school_master__failed_manual_checks_job,
    school_master__successful_manual_checks_job,
    school_reference__convert_gold_csv_to_deltatable_job,
)
from src.resources.io_managers import (
    ADLSBronzeIOManager,
    ADLSDeltaIOManager,
    ADLSRawIOManager,
)
from src.sensors import (
    qos__csv_to_deltatable_sensor,
    school_master__failed_manual_checks_sensor,
    school_master__gold_csv_to_deltatable_sensor,
    school_master__raw_file_uploads_sensor,
    school_master__successful_manual_checks_sensor,
    school_reference__gold_csv_to_deltatable_sensor,
)
from src.utils.adls import ADLSFileClient
from src.utils.sentry import setup_sentry
from src.utils.spark import pyspark

setup_sentry()


defs = Definitions(
    assets=[
        *load_assets_from_package_module(
            package_module=assets, group_name="school_master_data"
        ),
        *load_assets_from_package_module(package_module=qos, group_name="qos_data"),
        *load_assets_from_package_module(
            package_module=datahub_assets, group_name="datahub"
        ),
    ],
    resources={
        "adls_raw_io_manager": ADLSRawIOManager(pyspark=pyspark),
        "adls_bronze_io_manager": ADLSBronzeIOManager(pyspark=pyspark),
        "adls_delta_io_manager": ADLSDeltaIOManager(pyspark=pyspark),
        "adls_file_client": ADLSFileClient(),
        "gx": GEContextResource(ge_root_dir="src/resources/great_expectations"),
        "spark": pyspark,
    },
    jobs=[
        school_master__automated_data_checks_job,
        school_master__successful_manual_checks_job,
        school_master__failed_manual_checks_job,
        school_master__convert_gold_csv_to_deltatable_job,
        school_reference__convert_gold_csv_to_deltatable_job,
        qos__convert_csv_to_deltatable_job,
        datahub__ingest_azure_ad_users_groups_job,
        datahub__create_domains_job,
        datahub__create_tags_job,
        datahub__update_policies_job,
    ],
    sensors=[
        school_master__raw_file_uploads_sensor,
        school_master__successful_manual_checks_sensor,
        school_master__failed_manual_checks_sensor,
        school_master__gold_csv_to_deltatable_sensor,
        school_reference__gold_csv_to_deltatable_sensor,
        qos__csv_to_deltatable_sensor,
    ],
)
