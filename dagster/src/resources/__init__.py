from src.utils.adls import ADLSFileClient
from src.utils.db import PostgreSQLDatabase
from src.utils.spark import pyspark

from .io_managers.adls_delta import ADLSDeltaIOManager
from .io_managers.adls_delta_v2 import ADLSDeltaV2IOManager
from .io_managers.adls_json import ADLSJSONIOManager
from .io_managers.adls_pandas import ADLSPandasIOManager
from .io_managers.adls_passthrough import ADLSPassthroughIOManager

RESOURCE_DEFINITIONS = {
    "adls_delta_io_manager": ADLSDeltaIOManager(pyspark=pyspark),
    "adls_delta_v2_io_manager": ADLSDeltaV2IOManager(pyspark=pyspark),
    "adls_json_io_manager": ADLSJSONIOManager(),
    "adls_pandas_io_manager": ADLSPandasIOManager(pyspark=pyspark),
    "adls_passthrough_io_manager": ADLSPassthroughIOManager(),
    "adls_file_client": ADLSFileClient(),
    "spark": pyspark,
    "database": PostgreSQLDatabase(),
}
