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
