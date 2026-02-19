import asyncio

from dagster import HookContext, failure_hook
from src.utils.slack.send_slack_error_notification import send_slack_error_notification


@failure_hook
def slack_error_notification_hook(context: HookContext):
    """
    Sends a Slack notification when a pipeline step fails.

    This hook can be attached to any job to receive Slack notifications on failures.
    It extracts error information from the hook context and sends a formatted message
    to the configured Slack webhook.

    Usage:
        from src.hooks.slack_notification import slack_error_notification_hook

        my_job = define_asset_job(
            name="my_job",
            selection=["my_assets*"],
            hooks={slack_error_notification_hook})
    """

    job_name = context.job_name
    op_name = context.op.name if context.op else context.step_key
    run_id = context.run_id

    context.log.info(
        f"Sending Slack error notification for Job: {job_name}, Op: {op_name}, Run ID: {run_id}"
    )

    error_message = "Unknown error"
    if context.op_exception:
        error_message = str(context.op_exception)

    try:
        asyncio.run(
            send_slack_error_notification(
                job_name=job_name,
                op_name=op_name,
                run_id=run_id,
                error_message=error_message,
            )
        )
        context.log.info("Slack error notification sent successfully")
    except Exception as e:
        context.log.error(f"Failed to send Slack error notification: {e}")
