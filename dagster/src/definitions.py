from dagster import Definitions, load_assets_from_package_module
from src.assets import (
    adhoc,
    common,
    datahub_assets,
    migrations,
    qos,
    school_coverage,
    school_geolocation,
)
from src.jobs import (
    datahub__create_domains_job,
    datahub__create_tags_job,
    datahub__ingest_azure_ad_users_groups_job,
    datahub__ingest_coverage_notebooks_from_github_job,
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
from src.resources.io_managers import (
    ADLSDeltaIOManager,
    ADLSDeltaV2IOManager,
    ADLSJSONIOManager,
    ADLSPandasIOManager,
    ADLSPassthroughIOManager,
)
from src.resources.io_managers.adls_csv import AdlsCsvIOManager
from src.sensors.sensors import (
    qos__csv_to_deltatable_sensor,
    school_master__gold_csv_to_deltatable_sensor,
    school_master_coverage__failed_manual_checks_sensor,
    school_master_coverage__raw_file_uploads_sensor,
    school_master_coverage__successful_manual_checks_sensor,
    school_master_geolocation__failed_manual_checks_sensor,
    school_master_geolocation__raw_file_uploads_sensor,
    school_master_geolocation__successful_manual_checks_sensor,
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
        *load_assets_from_package_module(package_module=common, group_name="common"),
        *load_assets_from_package_module(package_module=qos, group_name="qos_data"),
        *load_assets_from_package_module(
            package_module=datahub_assets, group_name="datahub"
        ),
        *load_assets_from_package_module(
            package_module=migrations, group_name="migrations"
        ),
        *load_assets_from_package_module(package_module=adhoc, group_name="adhoc"),
    ],
    resources={
        "adls_delta_io_manager": ADLSDeltaIOManager(pyspark=pyspark),
        "adls_delta_v2_io_manager": ADLSDeltaV2IOManager(pyspark=pyspark),
        "adls_json_io_manager": ADLSJSONIOManager(),
        "adls_pandas_io_manager": ADLSPandasIOManager(pyspark=pyspark),
        "adls_passthrough_io_manager": ADLSPassthroughIOManager(),
        "pandas_csv_io_manager": AdlsCsvIOManager(pyspark=pyspark, engine="pandas"),
        "spark_csv_io_manager": AdlsCsvIOManager(pyspark=pyspark, engine="spark"),
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
        datahub__ingest_coverage_notebooks_from_github_job,
    ],
    sensors=[
        qos__csv_to_deltatable_sensor,
        school_master__gold_csv_to_deltatable_sensor,
        school_reference__gold_csv_to_deltatable_sensor,
        school_master_geolocation__raw_file_uploads_sensor,
        school_master_coverage__raw_file_uploads_sensor,
        school_master_geolocation__successful_manual_checks_sensor,
        school_master_coverage__successful_manual_checks_sensor,
        school_master_geolocation__failed_manual_checks_sensor,
        school_master_coverage__failed_manual_checks_sensor,
    ],
)
