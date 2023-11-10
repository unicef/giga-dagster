import os
from pathlib import Path

ENVIRONMENT = os.environ.get("ENVIRONMENT", "production")

BASE_DIR = Path(__file__).resolve().parent.parent

AZURE_SAS_TOKEN = os.environ.get("AZURE_SAS_TOKEN")

AZURE_BLOB_SAS_HOST = os.environ.get("AZURE_BLOB_SAS_HOST")

AZURE_BLOB_CONTAINER_NAME = os.environ.get("AZURE_BLOB_CONTAINER_NAME")

AZURE_STORAGE_ACCOUNT_NAME = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")

AZURE_BLOB_CONNECTION_URI = f"wasbs://{AZURE_BLOB_CONTAINER_NAME}@{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"

SPARK_RPC_AUTHENTICATION_SECRET = os.environ.get("SPARK_RPC_AUTHENTICATION_SECRET")
