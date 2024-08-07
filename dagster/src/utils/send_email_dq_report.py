import datetime
from typing import Any

import sentry_sdk
from models.file_upload import FileUpload
from sqlalchemy import select

from dagster import OpExecutionContext
from src.schemas.file_upload import FileUploadConfig
from src.utils.adls import ADLSFileClient
from src.utils.db.primary import get_db_context
from src.utils.email.send_email_base import send_email_base
from src.utils.logger import get_context_with_fallback_logger
from src.utils.op_config import FileConfig
from src.utils.sentry import log_op_context


def send_email_dq_report(
    dq_results: dict[str, Any],
    dataset_type: str,
    upload_date: str,
    upload_id: str,
    uploader_email: str,
    context: OpExecutionContext = None,
) -> None:
    metadata = {
        "dataset": dataset_type,
        "uploadDate": upload_date,
        "uploadId": upload_id,
        "dataQualityCheck": dq_results,
    }

    get_context_with_fallback_logger(context).info("SENDING DQ REPORT VIA EMAIL...")
    get_context_with_fallback_logger(context).info(metadata)

    send_email_base(
        endpoint="email/dq-report",
        props=metadata,
        subject="Giga Data Quality Report",
        recipients=[uploader_email],
        context=context,
    )


def send_email_dq_report_with_config(
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

        send_email_dq_report(
            dq_results=dq_results,
            dataset_type=dataset_type,
            upload_date=upload_date,
            upload_id=upload_id,
            uploader_email=uploader_email,
            context=context,
        )
    except Exception as error:
        get_context_with_fallback_logger(context).error(error)
        log_op_context(context)
        sentry_sdk.capture_exception(error=error)


if __name__ == "__main__":
    adls = ADLSFileClient()
    dq_results_filepath = "data-quality-results/school-coverage/dq-summary/AIA/cys5pwept28vrjsgu2bct7lg_AIA_coverage_fb_20240411-074343.json"
    dq_results = adls.download_json(dq_results_filepath)
    # print(json.dumps(dq_results, indent=2))

    dataset_type = "School Coverage"
    upload_date = datetime.datetime.now()
    upload_id = "cys5pwept28vrjsgu2bct7lg"
    uploader_email = "sofia.pineda@thinkingmachin.es"

    send_email_dq_report(
        dq_results=dq_results,
        dataset_type=dataset_type,
        upload_date=upload_date,
        upload_id=upload_id,
        uploader_email=uploader_email,
    )
