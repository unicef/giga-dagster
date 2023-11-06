from dagster_ge.factory import ge_data_context

from dagster import Definitions, fs_io_manager
from src.datasource1.assets.raw_bank_loans import raw__bank_loans
from src.settings import ENVIRONMENT
from src.transforms.assets.assets import (
    bronze,
    dq_failed_rows,
    dq_passed_rows,
    expectation_suite_asset,
    gold,
    manual_review_failed_rows,
    manual_review_passed_rows,
    raw_file,
    silver,
)
from src.transforms.assets.great_expectations import (
    ge_data_docs,
    test_expectation_suite_asset,
)
from src.transforms.jobs import (
    school_master__run_automated_data_checks_job,
    school_master__run_manual_checks_and_transforms_job,
)
from src.transforms.sensors import (
    school_master__run_automated_data_checks_sensor,
    school_master__run_manual_checks_and_transforms_sensor,
)

io_manager = (
    {
        "io_manager": fs_io_manager.configured({"base_dir": "/tmp/io_manager_storage"}),
    }
    if ENVIRONMENT != "production"
    else {}
)

defs = Definitions(
    assets=[
        raw_file,
        bronze,
        expectation_suite_asset,
        dq_failed_rows,
        dq_passed_rows,
        manual_review_failed_rows,
        manual_review_passed_rows,
        silver,
        gold,
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
