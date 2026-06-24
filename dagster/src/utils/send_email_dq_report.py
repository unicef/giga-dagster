import datetime
import math
from base64 import b64encode
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


def _entity_for_dataset(dataset: str) -> dict[str, str]:
    if dataset in {"health", "health-master"}:
        return {
            "plural": "Health Centers",
            "lowerPlural": "health centers",
            "lowerSingular": "health center",
        }
    return {
        "plural": "Schools",
        "lowerPlural": "schools",
        "lowerSingular": "school",
    }


def _make_json_safe(obj: Any) -> Any:
    """Replace float nan/inf with None so payload is JSON-serializable."""
    if isinstance(obj, dict):
        return {k: _make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_make_json_safe(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj


def _pdf_attachment_dict(pdf_bytes: bytes, filename: str) -> list[dict]:
    return [
        {
            "Content-type": "application/pdf",
            "Filename": filename,
            "content": b64encode(pdf_bytes).decode("ascii"),
        }
    ]


def _try_cached_pdf_attachment(
    adls: ADLSFileClient,
    pdf_path: str,
    country_code: str,
    upload_id: str,
    logger,
) -> list[dict] | None:
    try:
        adls.get_file_metadata(pdf_path)
        pdf_bytes = ADLSFileClient.download_raw(pdf_path)
        logger.info(f"Using existing DQ report PDF from ADLS at {pdf_path}")
        return _pdf_attachment_dict(
            pdf_bytes,
            f"data-quality-report-{country_code}-{upload_id}.pdf",
        )
    except Exception:
        logger.info(
            "No existing DQ report PDF found at %s; falling back to renderer",
            pdf_path,
        )
        return None


def _fetch_pdf_from_renderer(
    metadata: dict[str, Any],
    logger,
) -> requests.Response | None:
    try:
        return requests.post(
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


def _log_pdf_error_response(pdf_res: requests.Response, logger) -> None:
    logger.warning(f"Failed to generate PDF: {pdf_res.status_code}")
    try:
        logger.warning(f"PDF error response: {pdf_res.json()}")
    except JSONDecodeError:
        logger.warning(f"PDF error response: {pdf_res.text[:500]}")


def _is_valid_pdf_response(pdf_res: requests.Response, logger) -> bool:
    if not pdf_res.ok:
        _log_pdf_error_response(pdf_res, logger)
        return False

    pdf_bytes = pdf_res.content
    content_type = (pdf_res.headers.get("Content-Type") or "").split(";")[0].strip()
    if content_type != "application/pdf" or not pdf_bytes or len(pdf_bytes) < 1000:
        logger.warning(
            "Invalid PDF response: Content-Type=%s, len=%s",
            content_type,
            len(pdf_bytes) if pdf_bytes else 0,
        )
        return False
    return True


def _pdf_filename_from_response(
    pdf_res: requests.Response,
    country_code: str,
    upload_id: str,
) -> str:
    disp = pdf_res.headers.get("Content-Disposition") or ""
    pdf_filename = f"data-quality-report-{country_code}-{upload_id}.pdf"
    if "filename=" in disp:
        part = disp.split("filename=", 1)[1].strip().strip('"')
        if part:
            pdf_filename = part
    return pdf_filename


def _store_pdf_in_adls(
    adls: ADLSFileClient,
    pdf_bytes: bytes,
    pdf_path: str,
    logger,
) -> None:
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

    if not metadata.get("valueMaps"):
        cached = _try_cached_pdf_attachment(
            adls, pdf_path, country_code, upload_id, logger
        )
        if cached is not None:
            return cached
    else:
        logger.info(
            "Regenerating DQ report PDF (value maps present) instead of ADLS cache"
        )

    pdf_res = _fetch_pdf_from_renderer(metadata, logger)
    if pdf_res is None or not _is_valid_pdf_response(pdf_res, logger):
        return None

    pdf_bytes = pdf_res.content
    pdf_filename = _pdf_filename_from_response(pdf_res, country_code, upload_id)
    _store_pdf_in_adls(adls, pdf_bytes, pdf_path, logger)
    logger.info(f"PDF generated successfully: {pdf_filename}")
    return _pdf_attachment_dict(pdf_bytes, pdf_filename)


async def send_email_dq_report(
    dq_results: dict[str, Any],
    dataset: str,
    dataset_type: str,
    upload_date: str | datetime.datetime,
    upload_id: str,
    uploader_email: str,
    country_code: str = None,
    file_upload: FileUploadConfig | None = None,
    value_maps: dict[str, Any] | None = None,
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

    # valueMaps may be stored on the dq-summary blob; prefer explicit arg when given.
    pdf_value_maps = (
        value_maps if value_maps is not None else dq_results.get("valueMaps")
    )

    dq_check_payload = {
        k: v for k, v in dq_results_formatted.items() if k != "valueMaps"
    }

    metadata = {
        "dataset": dataset_type,
        "uploadDate": upload_date_str,
        "uploadId": upload_id,
        "dataQualityCheck": dq_check_payload,
    }

    if pdf_value_maps:
        metadata["valueMaps"] = pdf_value_maps

    # Add country if provided (required for PDF generation)
    if country_code:
        metadata["country"] = country_code

    if file_upload is not None:
        if file_upload.original_filename:
            metadata["uploadedFileName"] = file_upload.original_filename
        metadata["entity"] = _entity_for_dataset(file_upload.dataset)
        try:
            upload_meta = ADLSFileClient().fetch_metadata_for_blob(
                file_upload.upload_path
            )
            if upload_meta:
                metadata["uploadMetadata"] = {str(k): v for k, v in upload_meta.items()}
        except Exception:
            pass

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
    value_maps: dict[str, Any] | None = None,
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
            file_upload=file_upload,
            value_maps=value_maps,
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
