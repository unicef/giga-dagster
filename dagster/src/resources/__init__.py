from enum import Enum

from src.utils.adls import ADLSFileClient
from src.utils.spark import pyspark

from .io_managers.adls_delta import ADLSDeltaIOManager
from .io_managers.adls_generic_file import ADLSGenericFileIOManager
from .io_managers.adls_json import ADLSJSONIOManager
from .io_managers.adls_pandas import ADLSPandasIOManager
from .io_managers.adls_passthrough import ADLSPassthroughIOManager
from .io_managers.giga_meter_delta import GigaMeterDeltaIOManager
from .io_managers.adls_spark import ADLSSparkIOManager
from .io_managers.adls_spark_single_file import ADLSSparkSingleFileIOManager


class ResourceKey(Enum):
    ADLS_DELTA_IO_MANAGER = "adls_delta_io_manager"
    ADLS_GENERIC_FILE_IO_MANAGER = "adls_generic_file_io_manager"
    ADLS_JSON_IO_MANAGER = "adls_json_io_manager"
    ADLS_PANDAS_IO_MANAGER = "adls_pandas_io_manager"
    ADLS_PASSTHROUGH_IO_MANAGER = "adls_passthrough_io_manager"
    GIGA_METER_DELTA_IO_MANAGER = "giga_meter_delta_io_manager"
    ADLS_SPARK_IO_MANAGER = "adls_spark_io_manager"
    ADLS_SPARK_SINGLE_FILE_IO_MANAGER = "adls_spark_single_file_io_manager"
    ADLS_FILE_CLIENT = "adls_file_client"
    SPARK = "spark"


RESOURCE_DEFINITIONS = {
    ResourceKey.ADLS_DELTA_IO_MANAGER.value: ADLSDeltaIOManager(pyspark=pyspark),
    ResourceKey.ADLS_GENERIC_FILE_IO_MANAGER.value: ADLSGenericFileIOManager(),
    ResourceKey.ADLS_JSON_IO_MANAGER.value: ADLSJSONIOManager(),
    ResourceKey.ADLS_PANDAS_IO_MANAGER.value: ADLSPandasIOManager(pyspark=pyspark),
    ResourceKey.ADLS_PASSTHROUGH_IO_MANAGER.value: ADLSPassthroughIOManager(),
    ResourceKey.GIGA_METER_DELTA_IO_MANAGER.value: GigaMeterDeltaIOManager(pyspark=pyspark
    ),
    ResourceKey.ADLS_SPARK_IO_MANAGER.value: ADLSSparkIOManager(pyspark=pyspark),
    ResourceKey.ADLS_SPARK_SINGLE_FILE_IO_MANAGER.value: ADLSSparkSingleFileIOManager(
        pyspark=pyspark
    ),
    ResourceKey.ADLS_FILE_CLIENT.value: ADLSFileClient(),
    ResourceKey.SPARK.value: pyspark,
}
