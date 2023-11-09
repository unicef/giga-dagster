from dagster_ge.factory import ge_data_context

from dagster import Definitions, fs_io_manager, load_assets_from_package_module
from src._utils.io_managers import ADLSFileClient, StagingADLSIOManager
from src.assets import transforms

# from src.assets.transforms.great_expectations import (
#     ge_data_docs,
#     test_expectation_suite_asset,
# )
# from src.datasource1.assets.raw_bank_loans import raw__bank_loans
from src.jobs import (
    school_master__run_automated_data_checks_job,
    school_master__run_manual_checks_and_transforms_job,
)
from src.sensors import (
    school_master__run_automated_data_checks_sensor,
    school_master__run_manual_checks_and_transforms_sensor,
)
from src.settings import ENVIRONMENT

# from src.settings import ENVIRONMENT

# from src.datasource1.assets.raw_bank_loans import raw__bank_loans
# from src.settings import ENVIRONMENT
# from src.transforms.assets.great_expectations import (
#     expectation_suite_asset,
#     ge_data_docs,
# )

io_managers = {
    "dev": fs_io_manager.configured({"base_dir": "/tmp/io_manager_storage"}),
    "adls_staging": StagingADLSIOManager(),
}
defs = Definitions(
    assets=[
        *load_assets_from_package_module(
            package_module=transforms, group_name="school_master_data"
        ),
        # raw__bank_loans,
        # ge_data_docs,
        # test_expectation_suite_asset,
    ],
    resources={
        "ge_data_context": ge_data_context.configured(
            {"ge_root_dir": "src/transforms/resources/great-expectations"}
        ),
        "adls_io_manager": io_managers.get(f"adls_{ENVIRONMENT}"),
        "adls_loader": ADLSFileClient(),
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
