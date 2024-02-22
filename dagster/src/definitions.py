from dagster import Definitions, load_assets_from_package_module
from src import jobs, sensors
from src.assets import (
    adhoc,
    common,
    datahub_assets,
    migrations,
    qos,
    school_coverage,
    school_geolocation,
)
from src.resources.io_managers import (
    ADLSDeltaIOManager,
    ADLSDeltaV2IOManager,
    ADLSJSONIOManager,
    ADLSPandasIOManager,
    ADLSPassthroughIOManager,
)
from src.resources.io_managers.adls_csv import AdlsCsvIOManager
from src.utils.adls import ADLSFileClient
from src.utils.load_module import (
    load_jobs_from_package_module,
    load_sensors_from_package_module,
)
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
    jobs=[*load_jobs_from_package_module(jobs)],
    sensors=[*load_sensors_from_package_module(sensors)],
)
