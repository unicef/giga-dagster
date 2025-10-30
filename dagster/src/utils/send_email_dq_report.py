import datetime
from typing import Any

import requests
import sentry_sdk
from models.file_upload import FileUpload
from requests import HTTPError, JSONDecodeError
from sqlalchemy import select

from dagster import OpExecutionContext
from src.internal.groups import GroupsApi
from src.schemas.file_upload import FileUploadConfig
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.db.primary import get_db_context
from src.utils.email.send_email_base import send_email_base
from src.utils.logger import get_context_with_fallback_logger
from src.utils.op_config import FileConfig
from src.utils.sentry import log_op_context


async def send_email_dq_report(
    dq_results: dict[str, Any],
    dataset_type: str,
    upload_date: str | datetime.datetime,
    upload_id: str,
    uploader_email: str,
    country_code: str = None,
    context: OpExecutionContext = None,
) -> None:
    # Prepare metadata for email renderer
    # Convert upload_date to ISO format string if it's a datetime object
    if isinstance(upload_date, datetime.datetime):
        upload_date_str = upload_date.isoformat()
    else:
        upload_date_str = str(upload_date)
    
    # Format data quality check timestamp if present
    dq_results_formatted = dq_results.copy()
    if isinstance(dq_results_formatted.get("summary", {}).get("timestamp"), datetime.datetime):
        dq_results_formatted["summary"]["timestamp"] = dq_results_formatted["summary"]["timestamp"].isoformat()
    elif "summary" in dq_results_formatted and isinstance(dq_results_formatted["summary"].get("timestamp"), str):
        # Already a string, keep it as is
        pass
    
    metadata = {
        "dataset": dataset_type,
        "uploadDate": upload_date_str,
        "uploadId": upload_id,
        "dataQualityCheck": dq_results_formatted,
    }
    
    # Add country if provided (required for PDF generation)
    if country_code:
        metadata["country"] = country_code

    admins = GroupsApi.list_role_members("Admin")
    recipients = list({uploader_email, *admins})

    logger = get_context_with_fallback_logger(context)
    logger.info("SENDING DQ REPORT VIA EMAIL WITH PDF ATTACHMENT...")
    logger.info(metadata)
    logger.info(f"Recipients: {recipients}")

    # Generate PDF attachment if country is provided
    attachments = None
    if country_code:
        try:
            pdf_res = requests.post(
                f"{settings.EMAIL_RENDERER_SERVICE_URL}/email/dq-report-pdf",
                headers={
                    "Authorization": f"Bearer {settings.EMAIL_RENDERER_BEARER_TOKEN}",
                    "Content-Type": "application/json",
                },
                json=metadata,
                timeout=int(datetime.timedelta(minutes=2).total_seconds()),
            )

            if pdf_res.ok:
                pdf_data = pdf_res.json()
                pdf_base64 = pdf_data.get("pdf")
                pdf_filename = pdf_data.get(
                    "filename", f"data-quality-report-{country_code}-{upload_id}.pdf"
                )

                # Create attachment dict with base64-encoded content
                attachments = [{
                    "Content-type": "application/pdf",
                    "Filename": pdf_filename,
                    "content": pdf_base64,  # Already base64-encoded from email service
                }]
                logger.info(f"PDF generated successfully: {pdf_filename}")
            else:
                logger.warning(f"Failed to generate PDF: {pdf_res.status_code}")
                try:
                    logger.warning(f"PDF error response: {pdf_res.json()}")
                except JSONDecodeError:
                    logger.warning(f"PDF error response: {pdf_res.text}")
        except Exception as pdf_error:
            logger.error(f"Error generating PDF attachment: {pdf_error}")
            # Continue without PDF attachment if generation fails

    # Send email with PDF attachment (send_email_base will handle HTML/text rendering)
    await send_email_base(
        endpoint="email/dq-report",
        props=metadata,
        subject="Giga Data Quality Report",
        recipients=recipients,
        context=context,
        attachments=attachments,
    )


async def send_email_dq_report_with_config(
    dq_results: dict[str, Any],
    config: FileConfig,
    context: OpExecutionContext = None,
) -> None:
    try:
        with get_db_context() as db:
            file_upload = db.scalar(
                select(FileUpload).where(
                    FileUpload.id == config.filename_components.id
                ),
            )
            if file_upload is None:
                raise FileNotFoundError(
                    f"Database entry for FileUpload with id `{config.filename_components.id}` was not found",
                )

            file_upload = FileUploadConfig.from_orm(file_upload)

        domain = config.domain
        type = file_upload.dataset

        dataset_type = f"{domain} {type}".title()
        upload_date = file_upload.created
        upload_id = file_upload.id
        uploader_email = file_upload.uploader_email
        country_code = config.country_code if hasattr(config, 'country_code') else file_upload.country

        await send_email_dq_report(
            dq_results=dq_results,
            dataset_type=dataset_type,
            upload_date=upload_date,
            upload_id=upload_id,
            uploader_email=uploader_email,
            country_code=country_code,
            context=context,
        )
    except Exception as error:
        get_context_with_fallback_logger(context).error(error)
        log_op_context(context)
        sentry_sdk.capture_exception(error=error)


if __name__ == "__main__":
    import asyncio

    adls = ADLSFileClient()
    dq_results_filepath = "data-quality-results/school-coverage/dq-summary/AIA/cys5pwept28vrjsgu2bct7lg_AIA_coverage_fb_20240411-074343.json"
    dq_results = adls.download_json(dq_results_filepath)
    # print(json.dumps(dq_results, indent=2))

    dataset_type = "School Coverage"
    upload_date = datetime.datetime.now()
    upload_id = "cys5pwept28vrjsgu2bct7lg"
    uploader_email = "sofia.pineda@thinkingmachin.es"

    asyncio.run(
        send_email_dq_report(
            dq_results=dq_results,
            dataset_type=dataset_type,
            upload_date=str(upload_date),
            upload_id=upload_id,
            uploader_email=uploader_email,
        )
    )
