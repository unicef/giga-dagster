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

    PYTHON_ENV: Environment = Environment.PRODUCTION
    BASE_DIR: Path = Path(__file__).resolve().parent.parent
    AZURE_SAS_TOKEN: str
    AZURE_BLOB_SAS_HOST: str
    AZURE_BLOB_CONTAINER_NAME: str
    AZURE_STORAGE_ACCOUNT_NAME: str
    SPARK_RPC_AUTHENTICATION_SECRET: str

    @property
    def IN_PRODUCTION(self):
        return self.PYTHON_ENV == Environment.PRODUCTION

    @property
    def AZURE_STORAGE_USE_EMULATOR(self) -> bool:
        return not self.IN_PRODUCTION

    @property
    def AZURE_BLOB_CONNECTION_URI(self) -> str:
        return f"wasbs://{self.AZURE_BLOB_CONTAINER_NAME}@{self.AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"


@lru_cache
def get_settings():
    return Settings()


settings = get_settings()
