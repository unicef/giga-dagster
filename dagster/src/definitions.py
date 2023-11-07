from dagster_ge.factory import ge_data_context

from dagster import Definitions, fs_io_manager, load_assets_from_package_module
from src.assets.transforms.great_expectations import (
    ge_data_docs,
    test_expectation_suite_asset,
)
from src.datasource1.assets.raw_bank_loans import raw__bank_loans
from src.jobs import (
    school_master__run_automated_data_checks_job,
    school_master__run_manual_checks_and_transforms_job,
)
from src.sensors import (
    school_master__run_automated_data_checks_sensor,
    school_master__run_manual_checks_and_transforms_sensor,
)
from src.settings import ENVIRONMENT

io_manager = (
    {
        "io_manager": fs_io_manager.configured({"base_dir": "/tmp/io_manager_storage"}),
    }
    if ENVIRONMENT != "production"
    else {}
)

defs = Definitions(
    assets=[
        *load_assets_from_package_module(
            package_module="transforms", group_name="school-master-data"
        ),
        raw__bank_loans,
        ge_data_docs,
        test_expectation_suite_asset,
    ],
    resources={
        "ge_data_context": ge_data_context.configured(
            {"ge_root_dir": "src/transforms/resources/great-expectations"}
        ),
        **io_manager,
    },
    jobs=[
        school_master__run_automated_data_checks_job,
        school_master__run_manual_checks_and_transforms_job,
    ],
    sensors=[
        school_master__run_automated_data_checks_sensor,
        school_master__run_manual_checks_and_transforms_sensor,
    ],
)
