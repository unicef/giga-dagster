from datetime import timedelta
from enum import StrEnum
from functools import lru_cache
from pathlib import Path

from datahub.metadata.schema_classes import FabricTypeClass
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
    AAD_AZURE_TENANT_ID: str
    AAD_AZURE_CLIENT_ID: str
    AAD_AZURE_CLIENT_SECRET: str
    SPARK_RPC_AUTHENTICATION_SECRET: str
    DATAHUB_OIDC_REDIRECT_URL: str
    DATAHUB_OIDC_CLIENT_ID: str
    DATAHUB_OIDC_TENANT_ID: str
    DATAHUB_OIDC_CLIENT_SECRET: str
    HIVE_METASTORE_URI: str
    EMAIL_RENDERER_BEARER_TOKEN: str
    INGESTION_POSTGRESQL_USERNAME: str
    INGESTION_POSTGRESQL_PASSWORD: str
    INGESTION_POSTGRESQL_DATABASE: str

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
    INGESTION_DB_PORT: int = 5432
    SPARK_DRIVER_CORES: int = 2
    SPARK_DRIVER_MEMORY_MB: int = 1365
    LICENSE_LIST: list[str] = ["Open Source", "Giga Analysis", "CC-BY-4.0"]
    WAREHOUSE_USERNAME: str = ""
    ADMIN_EMAIL: str = ""
    MLAB_DB_CONNECTION_STRING: str = ""
    PROCO_DB_CONNECTION_STRING: str = ""

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
    def GIGASYNC_API_URL(self) -> str:
        return (
            f"http://ingestion-portal-data-ingestion.ictd-ooi-ingestionportal-{self.DEPLOY_ENV.value}.svc.cluster.local:3000"
            if self.IN_PRODUCTION
            else "http://api:8000"
        )

    @property
    def ADLS_ENVIRONMENT(self) -> DeploymentEnvironment:
        return (
            DeploymentEnvironment.DEVELOPMENT
            if self.DEPLOY_ENV == DeploymentEnvironment.LOCAL
            else self.DEPLOY_ENV
        )

    @property
    def DATAHUB_ENVIRONMENT(self) -> FabricTypeClass:
        if self.DEPLOY_ENV == DeploymentEnvironment.STAGING:
            return FabricTypeClass.STG
        elif self.DEPLOY_ENV == DeploymentEnvironment.PRODUCTION:
            return FabricTypeClass.PROD
        else:
            return FabricTypeClass.DEV

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
        return int(timedelta(minutes=2).total_seconds()) if self.IN_PRODUCTION else 30

    @property
    def DEFAULT_SCHEDULE_CRON(self) -> str:
        return "*/5 * * * *" if self.IN_PRODUCTION else "*/1 * * * *"

    @property
    def SPARK_WAREHOUSE_PATH(self) -> str:
        if self.PYTHON_ENV == Environment.LOCAL:
            if self.WAREHOUSE_USERNAME:
                return f"warehouse-local-{self.WAREHOUSE_USERNAME}"
            return "warehouse-local"
        return "warehouse"

    @property
    def SPARK_WAREHOUSE_DIR(self) -> str:
        return f"{self.AZURE_BLOB_CONNECTION_URI}/{self.SPARK_WAREHOUSE_PATH}"

    @property
    def INGESTION_DB_HOST(self) -> str:
        if self.DEPLOY_ENV in [
            DeploymentEnvironment.STAGING,
            DeploymentEnvironment.PRODUCTION,
        ]:
            return f"postgres-postgresql-primary.ictd-ooi-ingestionportal-{self.DEPLOY_ENV.value}.svc.cluster.local"
        elif self.DEPLOY_ENV == DeploymentEnvironment.DEVELOPMENT:
            return f"postgres-postgresql.ictd-ooi-ingestionportal-{self.DEPLOY_ENV.value}.svc.cluster.local"
        return "db"

    @property
    def INGESTION_DATABASE_CONNECTION_DICT(self) -> dict:
        return {
            "user": self.INGESTION_POSTGRESQL_USERNAME,
            "password": self.INGESTION_POSTGRESQL_PASSWORD,
            "host": self.INGESTION_DB_HOST,
            "port": str(self.INGESTION_DB_PORT),
            "path": f"/{self.INGESTION_POSTGRESQL_DATABASE}",
        }

    @property
    def INGESTION_DATABASE_URL(self) -> str:
        return str(
            PostgresDsn.build(
                scheme="postgresql+psycopg2",
                **self.INGESTION_DATABASE_CONNECTION_DICT,
            ),
        )


@lru_cache
def get_settings():
    return Settings()


settings = get_settings()
