import functools
import socket
from inspect import iscoroutinefunction, signature

import sentry_sdk
from sentry_sdk.integrations.argv import ArgvIntegration
from sentry_sdk.integrations.atexit import AtexitIntegration
from sentry_sdk.integrations.dedupe import DedupeIntegration
from sentry_sdk.integrations.logging import LoggingIntegration, ignore_logger
from sentry_sdk.integrations.modules import ModulesIntegration
from sentry_sdk.integrations.stdlib import StdlibIntegration

from dagster import OpExecutionContext, get_dagster_logger
from src.settings import settings

SENTRY_ENABLED = settings.IN_PRODUCTION and settings.SENTRY_DSN


def setup_sentry() -> None:
    if SENTRY_ENABLED:
        ignore_logger("dagster")

        sentry_sdk.init(
            dsn=settings.SENTRY_DSN,
            sample_rate=1.0,
            traces_sample_rate=1.0,
            environment=settings.DEPLOY_ENV.value,
            release=f"github.com/unicef/giga-dagster:{settings.COMMIT_SHA}",
            server_name=f"dagster-dagster-{settings.DEPLOY_ENV.name}@{socket.gethostname()}",
            default_integrations=False,
            integrations=[
                AtexitIntegration(),
                DedupeIntegration(),
                StdlibIntegration(),
                ModulesIntegration(),
                ArgvIntegration(),
                LoggingIntegration(),
            ],
        )


def log_op_context(context: OpExecutionContext) -> None:
    sentry_sdk.add_breadcrumb(
        category="dagster",
        message=f"{context.job_name} - {context.op_def.name}",
        level="info",
        data={
            "run_config": context.run_config,
            "job_name": context.job_name,
            "op_name": context.op_def.name,
            "run_id": context.run_id,
            "run_tags": context.run_tags,
            "retry_number": context.retry_number,
            "asset_key": context.asset_key,
        },
    )


def capture_op_exceptions(func: callable) -> callable:
    def create_wrapper(is_async: bool):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> signature(func).return_annotation:
            if not SENTRY_ENABLED:
                return (
                    await func(*args, **kwargs) if is_async else func(*args, **kwargs)
                )

            logger = get_dagster_logger("sentry")

            try:
                log_op_context(args[0])
            except (AttributeError, IndexError):
                logger.warning("Sentry did not find execution context as the first arg")

            try:
                result = (
                    await func(*args, **kwargs) if is_async else func(*args, **kwargs)
                )
                return result
            except Exception as e:
                event_id = sentry_sdk.capture_exception(e)
                logger.error(f"Sentry captured an exception. Event ID: {event_id}")
                raise e

        return wrapper

    is_async = iscoroutinefunction(func)
    return create_wrapper(is_async)
