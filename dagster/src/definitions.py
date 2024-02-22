from dagster import Definitions, load_assets_from_package_module
from src import jobs, sensors
from src.assets import (
    common,
    datahub_assets,
    qos,
    school_coverage,
    school_geolocation,
)
from src.resources.io_managers import (
    ADLSDeltaIOManager,
    ADLSJSONIOManager,
    ADLSPandasIOManager,
)
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
    ],
    resources={
        "adls_delta_io_manager": ADLSDeltaIOManager(pyspark=pyspark),
        "adls_json_io_manager": ADLSJSONIOManager(),
        "adls_pandas_io_manager": ADLSPandasIOManager(pyspark=pyspark),
        "adls_file_client": ADLSFileClient(),
        "spark": pyspark,
    },
    jobs=[*load_jobs_from_package_module(jobs)],
    sensors=[*load_sensors_from_package_module(sensors)],
)
