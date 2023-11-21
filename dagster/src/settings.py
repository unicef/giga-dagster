import os
from pathlib import Path

PYTHON_ENV = os.environ.get("PYTHON_ENV", "production")

IN_PRODUCTION = PYTHON_ENV == "production"

ENVIRONMENT = os.environ.get("ENVIRONMENT", "staging")

BASE_DIR = Path(__file__).resolve().parent.parent

AZURE_SAS_TOKEN = os.environ.get("AZURE_SAS_TOKEN")

AZURE_BLOB_SAS_HOST = os.environ.get("AZURE_BLOB_SAS_HOST")

AZURE_BLOB_CONTAINER_NAME = os.environ.get("AZURE_BLOB_CONTAINER_NAME")

AZURE_STORAGE_ACCOUNT_NAME = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")

AZURE_BLOB_CONNECTION_URI = f"wasbs://{AZURE_BLOB_CONTAINER_NAME}@{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"

SPARK_RPC_AUTHENTICATION_SECRET = os.environ.get("SPARK_RPC_AUTHENTICATION_SECRET")

DATAHUB_METADATA_SERVER_URL = os.environ.get("DATAHUB_METADATA_SERVER_URL")

DATAHUB_ACCESS_TOKEN = os.environ.get("DATAHUB_ACCESS_TOKEN")

SENTRY_DSN = os.environ.get("SENTRY_DSN")
