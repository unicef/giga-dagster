"""Tests for send_slack_master_release_notification module."""

from unittest.mock import AsyncMock, patch

import pytest
from src.utils.send_slack_master_release_notification import (
    SlackProps,
    format_changes_for_slack_message,
    send_slack_master_release_notification,
)


def test_format_changes_for_slack_message(spark_session):
    """Test formatting of changes for Slack message."""
    data = [
        ("column1", "added", 5),
        ("column2", "modified", 3),
        ("column3", "deleted", 2),
    ]
    df = spark_session.createDataFrame(
        data, ["column_name", "operation", "change_count"]
    )

    result = format_changes_for_slack_message(df)

    assert "column1" in result
    assert "added" in result
    assert "column2" in result
    assert "modified" in result
    assert "```" in result  # Code block formatting


@pytest.mark.asyncio
async def test_send_slack_master_release_notification():
    """Test sending Slack notification with all fields."""
    props = SlackProps(
        country="BRA",
        added=10,
        modified=5,
        deleted=2,
        updateDate="2023-01-01",
        version=1,
        rows=100,
        column_changes="No changes",
    )

    with patch(
        "src.utils.send_slack_master_release_notification.send_slack_base",
        new_callable=AsyncMock,
    ) as mock_send:
        await send_slack_master_release_notification(props)
        mock_send.assert_called_once()
        call_args = mock_send.call_args[0][0]
        assert "BRA" in call_args
        assert "10" in call_args
        assert "5" in call_args
        assert "2" in call_args


@pytest.mark.asyncio
async def test_send_slack_master_release_notification_zero_changes():
    """Test sending Slack notification with zero changes."""
    props = SlackProps(
        country="PHL",
        added=0,
        modified=0,
        deleted=0,
        updateDate="2023-01-01",
        version=2,
        rows=50,
        column_changes="No changes",
    )

    with patch(
        "src.utils.send_slack_master_release_notification.send_slack_base",
        new_callable=AsyncMock,
    ) as mock_send:
        await send_slack_master_release_notification(props)
        mock_send.assert_called_once()
        call_args = mock_send.call_args[0][0]
        assert "PHL" in call_args
        # When added/modified/deleted are 0, they should not appear in message
        assert "*Added*" not in call_args
        assert "*Modified*" not in call_args
        assert "*Deleted*" not in call_args


@pytest.mark.asyncio
async def test_send_slack_master_release_notification_partial_changes():
    """Test sending Slack notification with only some changes."""
    props = SlackProps(
        country="IND",
        added=5,
        modified=0,
        deleted=1,
        updateDate="2023-06-15",
        version=3,
        rows=200,
        column_changes="Some changes",
    )

    with patch(
        "src.utils.send_slack_master_release_notification.send_slack_base",
        new_callable=AsyncMock,
    ) as mock_send:
        await send_slack_master_release_notification(props)
        mock_send.assert_called_once()
        call_args = mock_send.call_args[0][0]
        assert "IND" in call_args
        assert "*Added*: 5" in call_args
        assert "*Deleted*: 1" in call_args
        # Modified is 0, so it should not appear
        assert "*Modified*" not in call_args
