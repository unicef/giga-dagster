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
    renderer_base = str(settings.EMAIL_RENDERER_SERVICE_URL).rstrip("/")
    renderer_url = f"{renderer_base}/{endpoint}"

    res = requests.post(
        renderer_url,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {settings.EMAIL_RENDERER_BEARER_TOKEN}",
        },
        json=props,
        timeout=int(timedelta(minutes=2).total_seconds()),
    )
    if not res.ok:
        logger.error(
            "Email renderer failed: status=%s body=%s",
            res.status_code,
            res.text[:500] if res.text else "",
        )
        try:
            raise HTTPError(res.json())
        except JSONDecodeError:
            raise HTTPError(res.text) from None

    data = res.json()
    logger.info("Email renderer response: %s", res.status_code)

    html = data.get("html")
    text = data.get("text")
    if not html and not text:
        logger.warning("Email renderer returned no html or text; email may not be sent")

    api_base = settings.GIGASYNC_API_URL
    logger.info("Calling Giga Sync API send-email at %s/api/email/send-email", api_base)
    async with AsyncClient(base_url=api_base) as client:
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
            logger.error(
                "Giga Sync API send-email failed: status=%s body=%s",
                res.status_code,
                res.text[:500] if res.text else "",
            )
            try:
                logger.error("API error body: %s", res.json())
            except Exception:
                pass
            res.raise_for_status()
        else:
            logger.info("Send email response: %s %s", res.status_code, res.text)
