"""Tests for send_email_master_release_notification module."""

from unittest.mock import AsyncMock, patch

import pytest
from src.utils.send_email_master_release_notification import (
    EmailProps,
    send_email_master_release_notification,
)


@pytest.mark.asyncio
async def test_send_email_master_release_notification():
    """Test sending email notification with valid recipients."""
    props = EmailProps(
        country="BRA",
        added=10,
        modified=5,
        deleted=2,
        updateDate="2023-01-01",
        version=1,
        rows=100,
    )
    recipients = ["admin@example.com", "user@example.com"]

    with patch(
        "src.utils.send_email_master_release_notification.send_email_base",
        new_callable=AsyncMock,
    ) as mock_send:
        await send_email_master_release_notification(props, recipients)
        mock_send.assert_called_once()


@pytest.mark.asyncio
async def test_send_email_master_release_notification_empty_recipients():
    """Test that email is not sent when recipients list is empty."""
    props = EmailProps(
        country="PHL",
        added=5,
        modified=3,
        deleted=1,
        updateDate="2023-01-01",
        version=2,
        rows=50,
    )
    recipients = []

    with (
        patch(
            "src.utils.send_email_master_release_notification.send_email_base",
            new_callable=AsyncMock,
        ) as mock_send,
        patch("src.utils.send_email_master_release_notification.logger") as mock_logger,
    ):
        await send_email_master_release_notification(props, recipients)
        mock_send.assert_not_called()
        mock_logger.warning.assert_called_once()


@pytest.mark.asyncio
async def test_send_email_master_release_notification_single_recipient():
    """Test sending email with single recipient."""
    props = EmailProps(
        country="IND",
        added=0,
        modified=0,
        deleted=0,
        updateDate="2023-06-15",
        version=3,
        rows=200,
    )
    recipients = ["single@example.com"]

    with patch(
        "src.utils.send_email_master_release_notification.send_email_base",
        new_callable=AsyncMock,
    ) as mock_send:
        await send_email_master_release_notification(props, recipients)
        mock_send.assert_called_once()
        # Check that props.dict() was passed
        call_args = mock_send.call_args[1]
        assert call_args["props"]["country"] == "IND"
        assert call_args["subject"] == "Master Data Update Notification"
