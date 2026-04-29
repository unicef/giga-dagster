from unittest.mock import MagicMock

import loguru
from src.utils.logger import (
    ContextLoggerWithLoguruFallback,
    get_context_with_fallback_logger,
)

from dagster import OpExecutionContext


def test_get_context_with_fallback_logger_with_context():
    mock_context = MagicMock(spec=OpExecutionContext)
    mock_context.log = MagicMock()
    logger = get_context_with_fallback_logger(mock_context)
    assert logger == mock_context.log


def test_get_context_with_fallback_logger_without_context():
    logger = get_context_with_fallback_logger(None)
    assert logger == loguru.logger


def test_context_logger_with_context():
    mock_context = MagicMock(spec=OpExecutionContext)
    mock_context.log = MagicMock()
    logger_wrapper = ContextLoggerWithLoguruFallback(context=mock_context)
    assert logger_wrapper.log == mock_context.log


def test_context_logger_without_context():
    logger_wrapper = ContextLoggerWithLoguruFallback()
    assert logger_wrapper.log == loguru.logger


def test_context_logger_passthrough():
    mock_context = MagicMock(spec=OpExecutionContext)
    mock_context.log = MagicMock()
    logger_wrapper = ContextLoggerWithLoguruFallback(context=mock_context)
    result = logger_wrapper.passthrough(42, "Test message")
    assert result == 42
    mock_context.log.info.assert_called_with("Test message")


def test_context_logger_passthrough_with_group():
    mock_context = MagicMock(spec=OpExecutionContext)
    mock_context.log = MagicMock()
    logger_wrapper = ContextLoggerWithLoguruFallback(
        context=mock_context, group="TestGroup"
    )
    result = logger_wrapper.passthrough(42, "Test message")
    assert result == 42
    mock_context.log.info.assert_called_with("[TestGroup] Test message")


# Negative test cases
def test_context_logger_passthrough_without_message():
    """Edge case: passthrough with None message should still work."""
    mock_context = MagicMock(spec=OpExecutionContext)
    mock_context.log = MagicMock()
    logger_wrapper = ContextLoggerWithLoguruFallback(context=mock_context)
    result = logger_wrapper.passthrough("value", None)
    assert result == "value"


def test_context_logger_passthrough_with_empty_message():
    """Edge case: passthrough with empty string message."""
    mock_context = MagicMock(spec=OpExecutionContext)
    mock_context.log = MagicMock()
    logger_wrapper = ContextLoggerWithLoguruFallback(context=mock_context)
    result = logger_wrapper.passthrough([1, 2, 3], "")
    assert result == [1, 2, 3]
    mock_context.log.info.assert_called_with("")


def test_context_logger_fallback_log_method():
    """Test that fallback logger is used when context is None for passthrough."""
    logger_wrapper = ContextLoggerWithLoguruFallback()
    # Should not raise any errors
    result = logger_wrapper.passthrough({"key": "value"}, "Test message")
    assert result == {"key": "value"}
