import sentry_sdk
from sentry_sdk.integrations.argv import ArgvIntegration
from sentry_sdk.integrations.atexit import AtexitIntegration
from sentry_sdk.integrations.dedupe import DedupeIntegration
from sentry_sdk.integrations.logging import LoggingIntegration, ignore_logger
from sentry_sdk.integrations.modules import ModulesIntegration
from sentry_sdk.integrations.spark import SparkIntegration
from sentry_sdk.integrations.stdlib import StdlibIntegration

from src.settings import IN_PRODUCTION, PYTHON_ENV, SENTRY_DSN

ignore_logger("dagster")


def setup_sentry():
    if not (IN_PRODUCTION and SENTRY_DSN):
        return

    sentry_sdk.init(
        dsn=SENTRY_DSN,
        traces_sample_rate=1.0,
        profiles_sample_rate=1.0,
        environment=PYTHON_ENV,
        default_integrations=False,
        integrations=[
            AtexitIntegration(),
            DedupeIntegration(),
            StdlibIntegration(),
            ModulesIntegration(),
            ArgvIntegration(),
            LoggingIntegration(),
            SparkIntegration(),
        ],
    )


if __name__ == "__main__":
    import pyspark.daemon as original_daemon

    setup_sentry()
    original_daemon.manager()
