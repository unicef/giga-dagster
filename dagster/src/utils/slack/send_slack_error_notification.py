import os

from httpx import AsyncClient

from dagster import OpExecutionContext
from src.settings import settings
from src.utils.logger import get_context_with_fallback_logger


async def send_slack_error_notification(
    job_name: str,
    op_name: str,
    run_id: str,
    error_message: str,
    context: OpExecutionContext = None,
):
    """Send a formatted error notification to Slack when a pipeline fails."""
    logger = get_context_with_fallback_logger(context)

    slack_webhook = os.getenv("SLACK_ERROR_NOTIFICATION_WEBHOOK", "")

    if not slack_webhook:
        logger.warning("SLACK_WEBHOOK not configured, skipping error notification")
        return

    text = ":x: *Pipeline Failure Alert*\n\n"
    text += f"*Environment:* `{settings.DEPLOY_ENV.value.upper()}`\n"
    text += f"*Job:* `{job_name}`\n"
    text += f"*Step:* `{op_name}`\n"
    text += f"*Run ID:* `{run_id}`\n\n"
    text += f"*Error:*\n```{error_message[:1500]}```\n"

    dagster_url = settings.DAGSTER_INGRESS_HOST
    text += f"\n<{dagster_url}/runs/{run_id}|View Run in Dagster>"

    async with AsyncClient() as client:
        res = await client.post(
            slack_webhook,
            json={"text": text},
        )
        if res.is_error:
            logger.error(f"Failed to send Slack error notification: {res.text}")
        else:
            logger.info(f"Slack error notification sent: {res.status_code}")
