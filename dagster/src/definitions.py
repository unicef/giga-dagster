from dagster_ge.factory import ge_data_context

from dagster import Definitions, load_assets_from_package_module
from src import assets
from src._utils.adls import ADLSFileClient
from src._utils.sentry import setup_sentry
from src._utils.spark import pyspark
from src.jobs import (
    school_master__run_automated_data_checks_job,
    school_master__run_failed_manual_checks_job,
    school_master__run_successful_manual_checks_job,
)
from src.resources.io_manager import StagingADLSIOManager
from src.sensors import (
    school_master__failed_manual_checks_sensor,
    school_master__raw_file_uploads_sensor,
    school_master__successful_manual_checks_sensor,
)

setup_sentry()


defs = Definitions(
    assets=[
        *load_assets_from_package_module(
            package_module=assets, group_name="school_master_data"
        ),
    ],
    resources={
        "ge_data_context": ge_data_context.configured(
            {"ge_root_dir": "src/resources/great_expectations"}
        ),
        "adls_io_manager": StagingADLSIOManager(pyspark=pyspark),
        "adls_file_client": ADLSFileClient(),
        "pyspark": pyspark,
    },
    jobs=[
        school_master__run_automated_data_checks_job,
        school_master__run_successful_manual_checks_job,
        school_master__run_failed_manual_checks_job,
    ],
    sensors=[
        school_master__raw_file_uploads_sensor,
        school_master__successful_manual_checks_sensor,
        school_master__failed_manual_checks_sensor,
    ],
)
