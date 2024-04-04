from datetime import timedelta

import requests
from loguru import logger
from requests import HTTPError, JSONDecodeError

from azure.communication.email import EmailClient
from src.settings import settings
from src.utils.adls import ADLSFileClient


def send_email_dq_report(json):
    client = EmailClient.from_connection_string(settings.AZURE_EMAIL_CONNECTION_STRING)

    res = requests.post(
        f"{settings.EMAIL_RENDERER_SERVICE_URL}/email/dq-report",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {settings.EMAIL_RENDERER_BEARER_TOKEN}",
        },
        json=json,
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
            "to": [
                {"address": recipient} for recipient in settings.EMAIL_TEST_RECIPIENTS
            ]
        },
        "content": {
            "subject": "Giga Test Email",
            "html": data.get("html"),
            "plainText": data.get("text"),
        },
    }
    poller = client.begin_send(message)
    result = poller.result()
    logger.info(result)


if __name__ == "__main__":
    adls = ADLSFileClient()
    dq_results_filepath = "logs-gx/test.json"
    dq_results_json = adls.download_json(dq_results_filepath)

    dataset_filepath = (
        "adls-testing-raw/school-coverage-data/BIH_school-coverage_gov_20240227.csv"
    )
    properties = adls.get_file_metadata(filepath=dataset_filepath)
    metadata = properties["metadata"]

    json_input = metadata | dq_results_json
    send_email_dq_report(json=json_input)
