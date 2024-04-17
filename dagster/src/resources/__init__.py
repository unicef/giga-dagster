from enum import Enum

from src.utils.adls import ADLSFileClient
from src.utils.spark import pyspark

from .io_managers.adls_delta import ADLSDeltaIOManager
from .io_managers.adls_json import ADLSJSONIOManager
from .io_managers.adls_pandas import ADLSPandasIOManager
from .io_managers.adls_passthrough import ADLSPassthroughIOManager


class ResourceKey(Enum):
    ADLS_DELTA_IO_MANAGER = "adls_delta_io_manager"
    ADLS_JSON_IO_MANAGER = "adls_json_io_manager"
    ADLS_PANDAS_IO_MANAGER = "adls_pandas_io_manager"
    ADLS_PASSTHROUGH_IO_MANAGER = "adls_passthrough_io_manager"
    ADLS_FILE_CLIENT = "adls_file_client"
    SPARK = "spark"


RESOURCE_DEFINITIONS = {
    ResourceKey.ADLS_DELTA_IO_MANAGER.value: ADLSDeltaIOManager(pyspark=pyspark),
    ResourceKey.ADLS_JSON_IO_MANAGER.value: ADLSJSONIOManager(),
    ResourceKey.ADLS_PANDAS_IO_MANAGER.value: ADLSPandasIOManager(pyspark=pyspark),
    ResourceKey.ADLS_PASSTHROUGH_IO_MANAGER.value: ADLSPassthroughIOManager(),
    ResourceKey.ADLS_FILE_CLIENT.value: ADLSFileClient(),
    ResourceKey.SPARK.value: pyspark,
}
