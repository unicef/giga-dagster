from enum import StrEnum
from functools import lru_cache
from pathlib import Path

from pydantic import BaseSettings


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
    AZURE_BLOB_SAS_HOST: str
    AZURE_DFS_SAS_HOST: str
    AZURE_BLOB_CONTAINER_NAME: str
    AZURE_STORAGE_ACCOUNT_NAME: str
    SPARK_RPC_AUTHENTICATION_SECRET: str
    # AUTH_OIDC_REDIRECT_URL: str
    # AUTH_OIDC_CLIENT_ID: str
    # AUTH_OIDC_TENANT_ID: str
    # AUTH_OIDC_CLIENT_SECRET: str

    # Settings with a default are not required to be in .env
    ENVIRONMENT: Environment = Environment.STAGING
    PYTHON_ENV: Environment = Environment.PRODUCTION
    BASE_DIR: Path = Path(__file__).resolve().parent.parent
    KUBERNETES_NAMESPACE: str = ""
    SENTRY_DSN: str = ""
    DATAHUB_ACCESS_TOKEN: str = ""
    SPARK_MASTER_HOST: str = "spark-master:7077"
    SHORT_SHA: str = ""

    # Derived settings
    @property
    def IN_PRODUCTION(self) -> bool:
        return self.PYTHON_ENV == Environment.PRODUCTION

    @property
    def AZURE_BLOB_CONNECTION_URI(self) -> str:
        return f"wasbs://{self.AZURE_BLOB_CONTAINER_NAME}@{self.AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"

    @property
    def DATAHUB_METADATA_SERVER_URL(self) -> str:
        return (
            f"http://datahub-datahub-gms.{self.KUBERNETES_NAMESPACE}:8080"
            if self.IN_PRODUCTION
            else "http://datahub-gms:8080"
        )


@lru_cache
def get_settings():
    return Settings()


settings = get_settings()
