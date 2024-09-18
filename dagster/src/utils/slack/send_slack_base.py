from httpx import AsyncClient
from src.settings import settings
from src.utils.logger import get_context_with_fallback_logger

from dagster import OpExecutionContext


async def send_slack_base(
    text: str,
    context: OpExecutionContext = None,
):
    logger = get_context_with_fallback_logger(context)

    async with AsyncClient() as client:
        res = await client.post(
            settings.SLACK_WEBHOOK,
            json={"text": text},
        )
        if res.is_error:
            logger.error(res.json())
            res.raise_for_status()
        else:
            logger.info(f"Send slack response: {res.status_code} {res.text}")
