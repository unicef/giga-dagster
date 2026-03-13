import datetime
import math
from base64 import b64decode, b64encode
from typing import Any

import requests
import sentry_sdk
from models.file_upload import FileUpload
from requests import JSONDecodeError
from sqlalchemy import select

from dagster import OpExecutionContext
from src.constants.constants_class import constants
from src.internal.groups import GroupsApi
from src.schemas.file_upload import FileUploadConfig
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.db.primary import get_db_context
from src.utils.email.send_email_base import send_email_base
from src.utils.logger import get_context_with_fallback_logger
from src.utils.op_config import FileConfig
from src.utils.sentry import log_op_context


def _make_json_safe(obj: Any) -> Any:
    """Replace float nan/inf with None so payload is JSON-serializable."""
    if isinstance(obj, dict):
        return {k: _make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_make_json_safe(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj


def _generate_pdf_attachment_and_store_in_adls(
    metadata: dict[str, Any],
    dataset: str,
    upload_id: str,
    country_code: str | None,
    logger,
) -> list[dict] | None:
    """Call renderer for PDF, store it in ADLS under dq-report, and build Mailjet attachment."""
    if not country_code:
        return None

    pdf_path = (
        f"{constants.dq_results_folder}/"
        f"{dataset}/dq-report/{country_code}/{upload_id}.pdf"
    )
    adls = ADLSFileClient()

    # Safety: try using an existing PDF from ADLS first
    try:
        adls.get_file_metadata(pdf_path)
        pdf_bytes_existing = ADLSFileClient.download_raw(pdf_path)
        pdf_base64_existing = b64encode(pdf_bytes_existing).decode("ascii")
        logger.info(f"Using existing DQ report PDF from ADLS at {pdf_path}")
        return [
            {
                "Content-type": "application/pdf",
                "Filename": f"data-quality-report-{country_code}-{upload_id}.pdf",
                "content": pdf_base64_existing,
            }
        ]
    except Exception:
        # If file doesn't exist or can't be read, fall back to renderer
        logger.info(
            "No existing DQ report PDF found at %s; falling back to renderer", pdf_path
        )

    # Call renderer to generate a fresh PDF (sanitize nan/inf so JSON is valid)
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
    except Exception as pdf_error:
        logger.error(f"Error calling email renderer for PDF: {pdf_error}")
        return None

    if not pdf_res.ok:
        logger.warning(f"Failed to generate PDF: {pdf_res.status_code}")
        try:
            logger.warning(f"PDF error response: {pdf_res.json()}")
        except JSONDecodeError:
            logger.warning(f"PDF error response: {pdf_res.text[:500]}")
        return None

    # Renderer returns binary PDF (avoids huge JSON/base64 truncation)
    pdf_bytes = pdf_res.content
    content_type = (pdf_res.headers.get("Content-Type") or "").split(";")[0].strip()
    if content_type != "application/pdf" or not pdf_bytes or len(pdf_bytes) < 1000:
        logger.warning(
            "Invalid PDF response: Content-Type=%s, len=%s",
            content_type,
            len(pdf_bytes) if pdf_bytes else 0,
        )
        return None

    # Filename from Content-Disposition or default
    disp = pdf_res.headers.get("Content-Disposition") or ""
    pdf_filename = (
        f"data-quality-report-{country_code}-{upload_id}.pdf"
    )
    if "filename=" in disp:
        part = disp.split("filename=", 1)[1].strip().strip('"')
        if part:
            pdf_filename = part

    # Store PDF in ADLS alongside DQ summary (dq-report path)
    try:
        adls.upload_raw(
            context=None,
            data=pdf_bytes,
            filepath=pdf_path,
            metadata=None,
        )
        logger.info(f"Stored DQ report PDF in ADLS at {pdf_path}")
    except Exception as adls_error:
        logger.error(f"Failed to store DQ report PDF in ADLS: {adls_error}")

    logger.info(f"PDF generated successfully: {pdf_filename}")
    pdf_base64 = b64encode(pdf_bytes).decode("ascii")
    return [
        {
            "Content-type": "application/pdf",
            "Filename": pdf_filename,
            "content": pdf_base64,
        }
    ]


async def send_email_dq_report(
    dq_results: dict[str, Any],
    dataset: str,
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
    if isinstance(
        dq_results_formatted.get("summary", {}).get("timestamp"), datetime.datetime
    ):
        dq_results_formatted["summary"]["timestamp"] = dq_results_formatted["summary"][
            "timestamp"
        ].isoformat()
    elif "summary" in dq_results_formatted and isinstance(
        dq_results_formatted["summary"].get("timestamp"), str
    ):
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

    # Ensure payload is JSON-serializable (replace float nan/inf with None)
    metadata = _make_json_safe(metadata)

    admins = GroupsApi.list_role_members("Admin")
    recipients = list({uploader_email, *admins})

    logger = get_context_with_fallback_logger(context)
    logger.info("SENDING DQ REPORT VIA EMAIL WITH PDF ATTACHMENT...")
    logger.info(metadata)
    logger.info(f"Recipients: {recipients}")

    # Generate PDF attachment if country is provided
    attachments = _generate_pdf_attachment_and_store_in_adls(
        metadata=metadata,
        dataset=dataset,
        upload_id=upload_id,
        country_code=country_code,
        logger=logger,
    )

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
        dataset_raw = file_upload.dataset

        dataset_type = f"{domain} {dataset_raw}".title()
        upload_date = file_upload.created
        upload_id = file_upload.id
        uploader_email = file_upload.uploader_email
        country_code = (
            config.country_code
            if hasattr(config, "country_code")
            else file_upload.country
        )

        await send_email_dq_report(
            dq_results=dq_results,
            dataset=f"{domain}-{dataset_raw}",
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
            dataset="school-coverage",
            dataset_type=dataset_type,
            upload_date=upload_date,
            upload_id=upload_id,
            uploader_email=uploader_email,
        )
    )
