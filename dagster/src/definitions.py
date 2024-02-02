from dagster import Definitions, load_assets_from_package_module
from src.assets import datahub_assets, qos, school_coverage, school_geolocation
from src.jobs import (
    datahub__create_domains_job,
    datahub__create_tags_job,
    datahub__ingest_azure_ad_users_groups_job,
    datahub__update_policies_job,
    qos__convert_csv_to_deltatable_job,
    school_master__convert_gold_csv_to_deltatable_job,
    school_master_coverage__automated_data_checks_job,
    school_master_coverage__failed_manual_checks_job,
    school_master_coverage__successful_manual_checks_job,
    school_master_geolocation__automated_data_checks_job,
    school_master_geolocation__failed_manual_checks_job,
    school_master_geolocation__successful_manual_checks_job,
    school_reference__convert_gold_csv_to_deltatable_job,
)
from src.resources.io_managers import ADLSDeltaIOManager, ADLSRawIOManager
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
            package_module=school_geolocation, group_name="school_geolocation_data"
        ),
        *load_assets_from_package_module(
            package_module=school_coverage, group_name="school_coverage_data"
        ),
        *load_assets_from_package_module(package_module=qos, group_name="qos_data"),
        *load_assets_from_package_module(
            package_module=datahub_assets, group_name="datahub"
        ),
    ],
    resources={
        "adls_raw_io_manager": ADLSRawIOManager(pyspark=pyspark),
        "adls_delta_io_manager": ADLSDeltaIOManager(pyspark=pyspark),
        "adls_file_client": ADLSFileClient(),
        "spark": pyspark,
    },
    jobs=[
        school_master_coverage__automated_data_checks_job,
        school_master_coverage__failed_manual_checks_job,
        school_master_coverage__successful_manual_checks_job,
        school_master_geolocation__automated_data_checks_job,
        school_master_geolocation__failed_manual_checks_job,
        school_master_geolocation__successful_manual_checks_job,
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
