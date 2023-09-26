from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    PYTHON_ENV: Literal["development", "staging", "production"] = "production"
    IN_PRODUCTION: bool = PYTHON_ENV == "production"
    BASE_DIR: Path = Path(__file__).parent.parent
    ALLOWED_HOSTS: list[str] = ["*"]
    CORS_ALLOWED_ORIGINS: list[str] = ["*"]
    SECRET_KEY: str
    AZURE_TENANT_ID: str
    AZURE_CLIENT_ID: str
    AZURE_CLIENT_SECRET: str
    AZURE_TENANT_NAME: str
    AZURE_REDIRECT_URI: str
    AZURE_AUTH_POLICY_NAME: str

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
