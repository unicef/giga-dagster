from datetime import timedelta
from enum import StrEnum
from functools import lru_cache
from pathlib import Path

from pydantic import BaseSettings, PostgresDsn


class Environment(StrEnum):
    LOCAL = "local"
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


class DeploymentEnvironment(StrEnum):
    LOCAL = "local"
    DEVELOPMENT = "dev"
    STAGING = "stg"
    PRODUCTION = "prd"


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
    HIVE_METASTORE_URI: str
    AZURE_EMAIL_CONNECTION_STRING: str
    EMAIL_RENDERER_BEARER_TOKEN: str
    EMAIL_TEST_RECIPIENTS: list[str]
    AZURE_EMAIL_SENDER: str
    INGESTION_POSTGRESQL_USERNAME: str
    INGESTION_POSTGRESQL_PASSWORD: str
    INGESTION_POSTGRESQL_DATABASE: str
    INGESTION_DB_HOST: str
    INGESTION_DB_PORT: str

    # Settings with a default are not required to be in .env
    PYTHON_ENV: Environment = Environment.PRODUCTION
    DEPLOY_ENV: DeploymentEnvironment = DeploymentEnvironment.LOCAL
    BASE_DIR: Path = Path(__file__).resolve().parent.parent
    SENTRY_DSN: str = ""
    DATAHUB_ACCESS_TOKEN: str = ""
    SPARK_MASTER_HOST: str = "spark-master"
    COMMIT_SHA: str = ""
    DATAHUB_METADATA_SERVER: str = ""
    EMAIL_RENDERER_SERVICE: str = ""
    GITHUB_ACCESS_TOKEN: str = ""

    # Derived settings
    @property
    def IN_PRODUCTION(self) -> bool:
        return self.PYTHON_ENV != Environment.LOCAL

    @property
    def DATAHUB_METADATA_SERVER_URL(self) -> str:
        return (
            f"http://datahub-datahub-gms.ictd-ooi-datahub-{self.DEPLOY_ENV.value}.svc.cluster.local:8080"
            if self.IN_PRODUCTION
            else self.DATAHUB_METADATA_SERVER
        )

    @property
    def EMAIL_RENDERER_SERVICE_URL(self) -> str:
        return (
            f"http://email-service.ictd-ooi-ingestionportal-{self.DEPLOY_ENV.value}.svc.cluster.local:3020"
            if self.IN_PRODUCTION
            else self.EMAIL_RENDERER_SERVICE
        )

    @property
    def ADLS_ENVIRONMENT(self) -> DeploymentEnvironment:
        return (
            DeploymentEnvironment.DEVELOPMENT
            if self.DEPLOY_ENV == DeploymentEnvironment.LOCAL
            else self.DEPLOY_ENV
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
        return int(timedelta(minutes=5).total_seconds()) if self.IN_PRODUCTION else 30

    @property
    def SPARK_WAREHOUSE_DIR(self) -> str:
        if self.PYTHON_ENV == Environment.LOCAL:
            return f"{self.AZURE_BLOB_CONNECTION_URI}/warehouse-local"
        return f"{self.AZURE_BLOB_CONNECTION_URI}/warehouse"

    @property
    def INGESTION_DATABASE_CONNECTION_DICT(self) -> dict:
        return {
            "username": self.INGESTION_POSTGRESQL_USERNAME,
            "password": self.INGESTION_POSTGRESQL_PASSWORD,
            "host": self.INGESTION_DB_HOST,
            "port": self.INGESTION_DB_PORT,
            "path": self.INGESTION_POSTGRESQL_DATABASE,
        }

    @property
    def INGESTION_DATABASE_URL(self) -> str:
        return str(
            PostgresDsn.build(
                scheme="postgresql+psycopg2",
                **self.INGESTION_DATABASE_CONNECTION_DICT,
            )
        )


@lru_cache
def get_settings():
    return Settings()


settings = get_settings()
