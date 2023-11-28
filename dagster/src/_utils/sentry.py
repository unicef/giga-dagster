import functools

import sentry_sdk
from sentry_sdk.integrations.argv import ArgvIntegration
from sentry_sdk.integrations.atexit import AtexitIntegration
from sentry_sdk.integrations.dedupe import DedupeIntegration
from sentry_sdk.integrations.logging import LoggingIntegration, ignore_logger
from sentry_sdk.integrations.modules import ModulesIntegration
from sentry_sdk.integrations.spark import SparkIntegration
from sentry_sdk.integrations.stdlib import StdlibIntegration

from dagster import OpExecutionContext, get_dagster_logger
from src.settings import settings

ignore_logger("dagster")


def setup_sentry():
    if not (settings.IN_PRODUCTION and settings.SENTRY_DSN):
        return

    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        traces_sample_rate=1.0,
        profiles_sample_rate=1.0,
        environment=settings.PYTHON_ENV,
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


def log_op_context(context: OpExecutionContext):
    sentry_sdk.add_breadcrumb(
        category="dagster",
        message=f"{context.job_name} - {context.op_def.name}",
        level="info",
        data=dict(
            run_config=context.run_config,
            job_name=context.job_name,
            op_name=context.op_def.name,
            run_id=context.run_id,
            retry_number=context.retry_number,
        ),
    )
    sentry_sdk.set_tag("job_name", context.job_name)
    sentry_sdk.set_tag("op_name", context.op_def.name)
    sentry_sdk.set_tag("run_id", context.run_id)


def capture_op_exceptions(func: callable):
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        logger = get_dagster_logger("sentry")

        try:
            log_op_context(args[0])
        except (AttributeError, IndexError):
            logger.warning(
                "Sentry did not find execution context as the first argument"
            )

        try:
            return func(*args, **kwargs)
        except Exception as e:
            event_id = sentry_sdk.capture_exception(e)
            logger.error(f"Exception captured by Sentry: {event_id=}")
            raise e

    return wrapped


if __name__ == "__main__":
    import pyspark.daemon as original_daemon

    setup_sentry()
    original_daemon.manager()
