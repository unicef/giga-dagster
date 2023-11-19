import sentry_sdk
from dagster_ge.factory import ge_data_context

from dagster import Definitions, fs_io_manager, load_assets_from_package_module
from src import assets
from src.jobs import (
    school_master__run_automated_data_checks_job,
    school_master__run_failed_manual_checks_job,
    school_master__run_successful_manual_checks_job,
)
from src.resources.adls_file_client import ADLSFileClient
from src.resources.io_manager import StagingADLSIOManager
from src.sensors import (
    school_master__failed_manual_checks_sensor,
    school_master__raw_file_uploads_sensor,
    school_master__successful_manual_checks_sensor,
)
from src.settings import ENVIRONMENT, IN_PRODUCTION, PYTHON_ENV, SENTRY_DSN

if IN_PRODUCTION and SENTRY_DSN:
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        traces_sample_rate=1.0,
        profiles_sample_rate=1.0,
        environment=PYTHON_ENV,
    )

io_managers = {
    "dev": fs_io_manager.configured({"base_dir": "/tmp/io_manager_storage"}),
    "adls_staging": StagingADLSIOManager(),
}

defs = Definitions(
    assets=[
        *load_assets_from_package_module(
            package_module=assets, group_name="school_master_data"
        ),
    ],
    resources={
        "ge_data_context": ge_data_context.configured(
            {"ge_root_dir": "src/transforms/resources/great-expectations"}
        ),
        "adls_io_manager": io_managers.get(f"adls_{ENVIRONMENT}"),
        "adls_file_client": ADLSFileClient(),
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
