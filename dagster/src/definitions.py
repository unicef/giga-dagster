import os

from dagster_ge.factory import ge_data_context

from dagster import Definitions, fs_io_manager
from src.datasource1.assets.raw_bank_loans import raw__bank_loans
from src.transforms.assets.great_expectations import (
    expectation_suite_asset,
    ge_data_docs,
)

io_managers = {
    "development": fs_io_manager.configured({"base_dir": "/tmp/io_manager_storage"}),
}

defs = Definitions(
    assets=[raw__bank_loans, expectation_suite_asset, ge_data_docs],
    resources={
        "io_manager": io_managers.get(os.getenv("ENVIRONMENT")),
        "ge_data_context": ge_data_context.configured(
            {"ge_root_dir": "src/transforms/resources/great-expectations"}
        ),
    },
    jobs=[],
    schedules=[],
)
