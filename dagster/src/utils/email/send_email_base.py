from datetime import timedelta
from typing import Any

import requests
from loguru import logger
from requests import HTTPError, JSONDecodeError

from azure.communication.email import EmailClient
from src.settings import settings


def send_email_base(
    endpoint: str, props: dict[str, Any], subject: str, recipients: list[str]
):
    client = EmailClient.from_connection_string(settings.AZURE_EMAIL_CONNECTION_STRING)
    res = requests.post(
        f"{settings.EMAIL_RENDERER_SERVICE_URL}/{endpoint}",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {settings.EMAIL_RENDERER_BEARER_TOKEN}",
        },
        json=props,
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
        "recipients": {"to": [{"address": recipient} for recipient in recipients]},
        "content": {
            "subject": subject,
            "html": data.get("html"),
            "plainText": data.get("text"),
        },
    }
    poller = client.begin_send(message)
    result = poller.result()
    logger.info(result)
