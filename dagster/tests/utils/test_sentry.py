from unittest.mock import MagicMock, patch

from src.utils.sentry import (
    capture_op_exceptions,
    log_op_context,
    setup_sentry,
)

from dagster import OpExecutionContext


@patch("src.utils.sentry.SENTRY_ENABLED", True)
@patch("src.utils.sentry.sentry_sdk.init")
@patch("src.utils.sentry.ignore_logger")
def test_setup_sentry_when_enabled(mock_ignore, mock_init):
    setup_sentry()
    mock_ignore.assert_called_once_with("dagster")
    mock_init.assert_called_once()


@patch("src.utils.sentry.SENTRY_ENABLED", False)
@patch("src.utils.sentry.sentry_sdk.init")
def test_setup_sentry_when_disabled(mock_init):
    setup_sentry()
    mock_init.assert_not_called()


@patch("src.utils.sentry.sentry_sdk.add_breadcrumb")
def test_log_op_context(mock_breadcrumb):
    mock_context = MagicMock(spec=OpExecutionContext)
    mock_context.job_name = "test_job"
    mock_context.op_def.name = "test_op"
    mock_context.run_id = "run123"
    mock_context.run_config = {}
    mock_context.run_tags = {}
    mock_context.retry_number = 0
    mock_context.asset_key = None
    log_op_context(mock_context)
    mock_breadcrumb.assert_called_once()


@patch("src.utils.sentry.SENTRY_ENABLED", False)
def test_capture_op_exceptions_disabled():
    @capture_op_exceptions
    def test_func(context):
        return "result"

    assert test_func is not None
    assert callable(test_func)
