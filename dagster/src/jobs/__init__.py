# pip install dagster_azure
from dagster_azure.adls2 import ADLS2PickleIOManager, ADLS2Resource

from dagster import Definitions, load_assets_from_modules

from . import assets

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "io_manager": ADLS2PickleIOManager(
            adls2_file_system="my-cool-fs",
            adls2_prefix="my-cool-prefix",
            adls2=ADLS2Resource(),
        ),
    },
)
