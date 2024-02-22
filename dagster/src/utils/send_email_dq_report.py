import requests
from loguru import logger
from pyspark import sql
from requests import HTTPError, JSONDecodeError

from azure.communication.email import EmailClient
from src.settings import settings


def send_email_dq_report(dq_report: sql.DataFrame):
    client = EmailClient.from_connection_string(settings.AZURE_EMAIL_CONNECTION_STRING)

    res = requests.post(
        "http://email-api:3020/email/dq-report",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {settings.EMAIL_RENDERER_BEARER_TOKEN}",
        },
        json={
            "name": "John Doe",
        },
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
