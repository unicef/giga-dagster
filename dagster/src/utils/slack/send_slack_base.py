from datetime import timedelta
from typing import Any

import requests
from httpx import AsyncClient
from requests import HTTPError, JSONDecodeError

from dagster import OpExecutionContext
from src.settings import settings
from src.utils.logger import get_context_with_fallback_logger


async def send_slack_base(
    text: str,
    context: OpExecutionContext = None,
):
    logger = get_context_with_fallback_logger(context)

    # response = requests.post(
    #     settings.SLACK_WEBHOOK,
    #     headers={
    #         "Content-Type": "application/json",
    #     },
    #     json=props,
    #     timeout=int(timedelta(minutes=2).total_seconds()),
    # )
    #
    # if response.code//100 != 2:
    #     logger.error(response.text)

    async with AsyncClient() as client:
        res = await client.post(
            settings.SLACK_WEBHOOK,
            json={'text': text},
        )
        if res.is_error:
            logger.error(res.json())
            res.raise_for_status()
        else:
            logger.info(f"Send slack response: {res.status_code} {res.text}")
