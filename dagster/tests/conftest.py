import os
import sys
from types import ModuleType

# src.settings is instantiated at import time. Tests use inert values so importing
# Dagster definitions never depends on a developer's .env or external services.
_REQUIRED_SETTINGS = {
    "AZURE_SAS_TOKEN": "test",
    "AZURE_BLOB_CONTAINER_NAME": "test",
    "AZURE_STORAGE_ACCOUNT_NAME": "test",
    "SPARK_RPC_AUTHENTICATION_SECRET": "test",
    "HIVE_METASTORE_URI": "thrift://localhost:9083",
    "EMAIL_RENDERER_BEARER_TOKEN": "test",
    "INGESTION_POSTGRESQL_USERNAME": "test",
    "INGESTION_POSTGRESQL_PASSWORD": "test",
    "INGESTION_POSTGRESQL_DATABASE": "test",
}

for name, value in _REQUIRED_SETTINGS.items():
    os.environ.setdefault(name, value)

# Fiona is only used when geospatial assets execute. Definition-contract tests
# import those assets but deliberately do not exercise external geometry I/O.
sys.modules.setdefault("fiona", ModuleType("fiona"))
