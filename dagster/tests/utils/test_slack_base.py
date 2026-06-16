"""Tests for send_slack_base module."""

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from src.utils.slack.send_slack_base import send_slack_base


@pytest.mark.asyncio
async def test_send_slack_base_success():
    """Test successful Slack message sending."""
    mock_response = MagicMock()
    mock_response.is_error = False
    mock_response.status_code = 200
    mock_response.text = "ok"

    with (
        patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post,
        patch(
            "src.utils.slack.send_slack_base.get_context_with_fallback_logger"
        ) as mock_get_logger,
    ):
        mock_post.return_value = mock_response
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        await send_slack_base(
            "Test message", webhook_url="https://hooks.slack.com/test"
        )

        mock_post.assert_called_once()
        mock_logger.info.assert_called_once()


@pytest.mark.asyncio
async def test_send_slack_base_error():
    """Test Slack message sending with error response."""
    mock_response = MagicMock()
    mock_response.is_error = True
    mock_response.status_code = 400
    mock_response.text = "invalid_payload"
    mock_response.json.return_value = {"error": "invalid_payload"}
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Error", request=MagicMock(), response=mock_response
    )

    with (
        patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post,
        patch(
            "src.utils.slack.send_slack_base.get_context_with_fallback_logger"
        ) as mock_get_logger,
        pytest.raises(httpx.HTTPStatusError),
    ):
        mock_post.return_value = mock_response
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        await send_slack_base(
            "Test message", webhook_url="https://hooks.slack.com/test"
        )

        mock_logger.error.assert_called_once()


@pytest.mark.asyncio
async def test_send_slack_base_with_context():
    """Test Slack message sending with context."""
    mock_context = MagicMock()
    mock_response = MagicMock()
    mock_response.is_error = False
    mock_response.status_code = 200
    mock_response.text = "ok"

    with (
        patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post,
        patch(
            "src.utils.slack.send_slack_base.get_context_with_fallback_logger"
        ) as mock_get_logger,
    ):
        mock_post.return_value = mock_response
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        await send_slack_base("Test message", context=mock_context)

        mock_get_logger.assert_called_once_with(mock_context)
        mock_logger.info.assert_called_once()
