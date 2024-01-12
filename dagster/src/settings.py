from enum import StrEnum
from functools import lru_cache
from pathlib import Path

from pydantic import BaseSettings, Field


class Environment(StrEnum):
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


class Settings(BaseSettings):
    class Config:
        env_file = ".env"
        extra = "ignore"

    # Settings required to be in .env
    AZURE_SAS_TOKEN: str
    AZURE_BLOB_CONTAINER_NAME: str
    AZURE_STORAGE_ACCOUNT_NAME: str
    SPARK_RPC_AUTHENTICATION_SECRET: str
    AUTH_OIDC_REDIRECT_URL: str
    AUTH_OIDC_CLIENT_ID: str
    AUTH_OIDC_TENANT_ID: str
    AUTH_OIDC_CLIENT_SECRET: str

    # Settings with a default are not required to be in .env
    ENVIRONMENT: Environment = Environment.STAGING
    PYTHON_ENV: Environment = Environment.PRODUCTION
    BASE_DIR: Path = Path(__file__).resolve().parent.parent
    DATAHUB_KUBERNETES_NAMESPACE: str = ""
    SENTRY_DSN: str = ""
    DATAHUB_ACCESS_TOKEN: str = ""
    SPARK_MASTER_HOST: str = "spark-master"
    SHORT_SHA: str = ""
    _DATAHUB_METADATA_SERVER: str = Field("", alias="DATAHUB_METADATA_SERVER")

    # Derived settings
    @property
    def IN_PRODUCTION(self) -> bool:
        return self.PYTHON_ENV == Environment.PRODUCTION

    @property
    def DATAHUB_METADATA_SERVER_URL(self) -> str:
        return (
            f"http://datahub-datahub-gms.{self.DATAHUB_KUBERNETES_NAMESPACE}:8080"
            if self.IN_PRODUCTION
            else self._DATAHUB_METADATA_SERVER
        )

    @property
    def AZURE_BLOB_SAS_HOST(self) -> str:
        return f"{self.AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"

    @property
    def AZURE_DFS_SAS_HOST(self) -> str:
        return f"{self.AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"

    @property
    def AZURE_BLOB_CONNECTION_URI(self) -> str:
        return f"wasbs://{self.AZURE_BLOB_CONTAINER_NAME}@{self.AZURE_BLOB_SAS_HOST}"

    @property
    def DEFAULT_SENSOR_INTERVAL_SECONDS(self) -> int:
        return 60 * 5 if self.IN_PRODUCTION else 30


@lru_cache
def get_settings():
    return Settings()


settings = get_settings()
