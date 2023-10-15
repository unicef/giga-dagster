from dagster import fs_io_manager

from .adls2_csv import Adls2CsvIOManager
from .adls2_delta import Adls2DeltaLakeIOManager

ALL_IO_MANAGERS = {
    "fs_io_manager": fs_io_manager.configured({"base_dir": "/tmp/io_manager_storage"}),
    "adls_csv_io_manager": Adls2CsvIOManager(),
    "adls_delta_io_manager": Adls2DeltaLakeIOManager(),
}
