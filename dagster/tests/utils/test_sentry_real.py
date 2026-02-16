from unittest.mock import ANY, MagicMock, patch

import pytest
from src.utils.sentry import capture_op_exceptions, log_op_context, setup_sentry


@pytest.fixture
def mock_sentry_sdk():
    with patch("src.utils.sentry.sentry_sdk") as mock:
        yield mock


@pytest.fixture
def mock_settings():
    with patch("src.utils.sentry.settings") as mock:
        mock.IN_PRODUCTION = True
        mock.SENTRY_DSN = "https://example.com"
        mock.DEPLOY_ENV.value = "test"
        mock.DEPLOY_ENV.name = "TEST"
        mock.COMMIT_SHA = "sha"
        yield mock


@pytest.fixture
def mock_sentry_enabled():
    with patch("src.utils.sentry.SENTRY_ENABLED", True):
        yield


def test_setup_sentry(mock_sentry_sdk, mock_settings, mock_sentry_enabled):
    setup_sentry()
    mock_sentry_sdk.init.assert_called_once()
    assert mock_sentry_sdk.init.call_args[1]["dsn"] == "https://example.com"


def test_log_op_context(mock_sentry_sdk):
    context = MagicMock()
    context.job_name = "test_job"
    context.op_def.name = "test_op"
    log_op_context(context)
    mock_sentry_sdk.add_breadcrumb.assert_called_with(
        category="dagster", message="test_job - test_op", level="info", data=ANY
    )


@pytest.mark.asyncio
async def test_capture_op_exceptions_sync(
    mock_sentry_sdk, mock_settings, mock_sentry_enabled
):
    @capture_op_exceptions
    def failing_func(context):
        raise ValueError("Boom")

    context = MagicMock()
    with pytest.raises(ValueError):
        await failing_func(context)
    mock_sentry_sdk.capture_exception.assert_called()


@pytest.mark.asyncio
async def test_capture_op_exceptions_async(
    mock_sentry_sdk, mock_settings, mock_sentry_enabled
):
    @capture_op_exceptions
    async def failing_func(context):
        raise ValueError("Boom Async")

    context = MagicMock()
    with pytest.raises(ValueError):
        await failing_func(context)
    mock_sentry_sdk.capture_exception.assert_called()
