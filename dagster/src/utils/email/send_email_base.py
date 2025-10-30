from datetime import timedelta
from typing import Any

import requests
from httpx import AsyncClient
from requests import HTTPError, JSONDecodeError

from dagster import OpExecutionContext
from src.schemas.email import GenericEmailRequest
from src.settings import settings
from src.utils.logger import get_context_with_fallback_logger


async def send_email_base(
    endpoint: str,
    props: dict[str, Any],
    subject: str,
    recipients: list[str],
    context: OpExecutionContext = None,
    attachments: list[dict] | None = None,
):
    logger = get_context_with_fallback_logger(context)

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
    logger.info(f"Email renderer response: {res.status_code}")

    html = data.get("html")
    text = data.get("text")

    async with AsyncClient(base_url=settings.GIGASYNC_API_URL) as client:
        res = await client.post(
            "/api/email/send-email",
            headers={"Authorization": f"Bearer {settings.EMAIL_RENDERER_BEARER_TOKEN}"},
            json=GenericEmailRequest(
                recipients=recipients,
                subject=subject,
                html_part=html,
                text_part=text,
                attachments=attachments,
            ).dict(),
        )
        if res.is_error:
            logger.error(res.json())
            res.raise_for_status()
        else:
            logger.info(f"Send email response: {res.status_code} {res.text}")
