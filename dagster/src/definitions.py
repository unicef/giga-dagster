import os

from dagster import Definitions, fs_io_manager

io_managers = {
    "dev": fs_io_manager.configured({"base_dir": "/tmp/io_manager_storage"}),
}

defs = Definitions(
    assets=[],
    resources={
        "io_manager": io_managers.get(os.getenv("ENVIRONMENT")),
    },
    jobs=[],
    schedules=[],
)
