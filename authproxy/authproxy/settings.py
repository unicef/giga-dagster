from datetime import timedelta
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    PYTHON_ENV: Literal["development", "staging", "production"] = "production"
    BASE_DIR: Path = Path(__file__).parent.parent
    SECRET_KEY: str
    AZURE_TENANT_ID: str
    AZURE_CLIENT_ID: str
    AZURE_CLIENT_SECRET: str
    AZURE_TENANT_NAME: str
    AZURE_REDIRECT_URI: str
    AZURE_LOGOUT_REDIRECT_URI: str
    AZURE_AUTH_POLICY_NAME: str
    DAGSTER_WEBSERVER_URL: str
    DAGSTER_WEBSERVER_READONLY_URL: str

    class Config:
        env_file = ".env"
        extra = "ignore"

    @property
    def IN_PRODUCTION(self):
        return self.PYTHON_ENV == "production"

    @property
    def ALLOWED_HOSTS(self):
        if self.IN_PRODUCTION:
            return ["io-airflow-dev.unitst.org"]
        return ["*"]

    @property
    def CORS_ALLOWED_ORIGINS(self):
        if self.IN_PRODUCTION:
            return ["io-airflow-dev.unitst.org"]
        return ["*"]

    @property
    def EMAIL_DOMAIN_ALLOWLIST(self):
        return ["thinkingmachin.es", "unicef.org"]

    @property
    def SESSION_COOKIE_MIDDLEWARE_PARAMS(self):
        return dict(
            max_age=int(timedelta(days=7).total_seconds()),
            same_site="strict",
            path="/",
            https_only=self.IN_PRODUCTION,
        )

    @property
    def SESSION_COOKIE_DELETE_PARAMS(self):
        return dict(
            samesite="strict",
            path="/",
            httponly=True,
            secure=self.IN_PRODUCTION,
        )

    @property
    def SESSION_COOKIE_PARAMS(self):
        return dict(
            max_age=int(timedelta(days=7).total_seconds()),
            **self.SESSION_COOKIE_DELETE_PARAMS,
        )

    @property
    def AUTHORITY_URL(self):
        # return f"https://{self.AZURE_TENANT_NAME}.b2clogin.com/{self.AZURE_TENANT_NAME}.onmicrosoft.com/{self.AZURE_AUTH_POLICY_NAME}"
        return "https://login.microsoftonline.com/common"

    @property
    def AZURE_AD_SCOPES(self):
        return [
            # f"https://{self.AZURE_TENANT_NAME}.onmicrosoft.com/{self.AZURE_CLIENT_ID}/User.Impersonate",
            "User.ReadBasic.All",
            "api://1e34b77a-70ed-4c14-a295-f0f47e4b84e1/user_impersonation",
        ]


@lru_cache()
def get_settings():
    return Settings()


settings = get_settings()
