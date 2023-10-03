from datetime import timedelta
from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    PYTHON_ENV: Literal["development", "staging", "production"] = "production"
    BASE_DIR: Path = Path(__file__).parent.parent
    ALLOWED_HOSTS: list[str] = ["*"]
    CORS_ALLOWED_ORIGINS: list[str] = ["*"]
    SECRET_KEY: str
    AZURE_TENANT_ID: str
    AZURE_CLIENT_ID: str
    AZURE_CLIENT_SECRET: str
    AZURE_TENANT_NAME: str
    AZURE_REDIRECT_URI: str
    AZURE_LOGOUT_REDIRECT_URI: str
    AZURE_AUTH_POLICY_NAME: str
    DAGSTER_WEBSERVER_URL: str

    class Config:
        env_file = ".env"
        extra = "ignore"

    @property
    def IN_PRODUCTION(self):
        return self.PYTHON_ENV == "production"

    @property
    def SESSION_COOKIE_PARAMS(self):
        return dict(
            max_age=int(timedelta(days=7).total_seconds()),
            same_site="strict",
            path="/",
            https_only=self.IN_PRODUCTION,
        )


settings = Settings()
