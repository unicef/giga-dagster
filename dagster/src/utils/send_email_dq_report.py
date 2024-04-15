import datetime
from datetime import timedelta
from typing import Any

import requests
from models.file_upload import FileUpload
from requests import HTTPError, JSONDecodeError
from sqlalchemy import select

from azure.communication.email import EmailClient
from dagster import OpExecutionContext
from src.schemas.file_upload import FileUploadConfig
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.db import get_db_context
from src.utils.logger import get_context_with_fallback_logger
from src.utils.op_config import FileConfig


def send_email_dq_report(
    dq_results: dict[str, Any],
    dataset_type: str,
    upload_date: datetime,
    upload_id: str,
    uploader_email: str,
    context: OpExecutionContext = None,
) -> None:
    metadata = {
        "dataset": dataset_type,
        "uploadDate": upload_date.isoformat(),
        "uploadId": upload_id,
        "dataQualityCheck": dq_results,
    }

    client = EmailClient.from_connection_string(settings.AZURE_EMAIL_CONNECTION_STRING)

    res = requests.post(
        f"{settings.EMAIL_RENDERER_SERVICE_URL}/email/dq-report",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {settings.EMAIL_RENDERER_BEARER_TOKEN}",
        },
        json=metadata,
        timeout=int(timedelta(minutes=2).total_seconds()),
    )
    if not res.ok:
        try:
            raise HTTPError(res.json())
        except JSONDecodeError:
            raise HTTPError(res.text) from None

    data = res.json()

    message = {
        "senderAddress": settings.AZURE_EMAIL_SENDER,
        "recipients": {
            "to": [{"address": uploader_email}],
        },
        "content": {
            "subject": "Giga Test Email",
            "html": data.get("html"),
            "plainText": data.get("text"),
        },
    }
    poller = client.begin_send(message)
    result = poller.result()
    get_context_with_fallback_logger(context).info(result)


def send_email_dq_report_with_config(
    dq_results: dict[str, Any],
    config: FileConfig,
    context: OpExecutionContext = None,
) -> None:
    with get_db_context() as db:
        file_upload = db.scalar(
            select(FileUpload).where(FileUpload.id == config.filename_components.id),
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
