from dagster_ge.factory import ge_data_context

from dagster import Definitions, fs_io_manager
from src.datasource1.assets.raw_bank_loans import raw__bank_loans
from src.settings import ENVIRONMENT
from src.transforms.assets.great_expectations import (
    expectation_suite_asset,
    ge_data_docs,
)

io_manager = (
    {
        "io_manager": fs_io_manager.configured({"base_dir": "/tmp/io_manager_storage"}),
    }
    if ENVIRONMENT != "production"
    else {}
)

defs = Definitions(
    assets=[raw__bank_loans, expectation_suite_asset, ge_data_docs],
    resources={
        "ge_data_context": ge_data_context.configured(
            {"ge_root_dir": "src/transforms/resources/great-expectations"}
        ),
        **io_manager,
    },
    jobs=[],
    schedules=[],
)
