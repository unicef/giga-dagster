from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from src.utils.send_email_dq_report import (
    send_email_dq_report,
    send_email_dq_report_with_config,
)


@pytest.mark.asyncio
async def test_send_email_dq_report():
    context = MagicMock()
    with (
        patch("src.utils.send_email_dq_report.send_email_base") as mock_send,
        patch("src.utils.send_email_dq_report.GroupsApi") as mock_groups,
    ):
        mock_groups.list_role_members.return_value = {"admin@example.com"}
        await send_email_dq_report(
            dq_results={"check": "pass"},
            dataset_type="Test",
            upload_date="2023-01-01",
            upload_id="123",
            uploader_email="user@example.com",
            context=context,
        )
        mock_send.assert_called_once()
        args = mock_send.call_args[1]
        assert "user@example.com" in args["recipients"]
        assert args["subject"] == "Giga Data Quality Report"


@pytest.mark.asyncio
async def test_send_email_dq_report_with_config():
    context = MagicMock()
    config = MagicMock()
    config.filename_components.id = "123"
    config.domain = "Domain"
    mock_upload = MagicMock()
    mock_upload.id = "123"
    mock_upload.dataset = "Dataset"
    mock_upload.created = "2023-01-01"
    mock_upload.uploader_email = "user@example.com"
    with (
        patch("src.utils.send_email_dq_report.get_db_context") as mock_db_ctx,
        patch(
            "src.utils.send_email_dq_report.send_email_dq_report",
            new_callable=AsyncMock,
        ) as mock_send_base,
        patch("src.utils.send_email_dq_report.FileUploadConfig") as mock_schema_config,
    ):
        mock_schema_object = MagicMock()
        mock_schema_object.dataset = "Dataset"
        mock_schema_object.created = "2023-01-01"
        mock_schema_object.id = "123"
        mock_schema_object.uploader_email = "user@example.com"
        mock_schema_config.from_orm.return_value = mock_schema_object
        mock_session = MagicMock()
        mock_db_ctx.return_value.__enter__.return_value = mock_session
        mock_session.scalar.return_value = mock_upload
        await send_email_dq_report_with_config(
            dq_results={}, config=config, context=context
        )
        mock_send_base.assert_called_once()
