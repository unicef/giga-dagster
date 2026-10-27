import asyncio
from collections import Counter
from datetime import UTC, datetime, timedelta

import sentry_sdk

from dagster import (
    DagsterEventType,
    DagsterRunStatus,
    DefaultSensorStatus,
    RunFailureSensorContext,
    RunsFilter,
    RunStatusSensorContext,
    run_failure_sensor,
    run_status_sensor,
)
from dagster._utils.error import SerializableErrorInfo
from src.jobs.analytics_tables import (
    analytics_tables_daily_job,
    analytics_tables_incremental_job,
)
from src.settings import DeploymentEnvironment, settings
from src.utils.slack.send_slack_base import send_slack_base

MAX_ERROR_LENGTH = 500


def _run_url(run_id: str) -> str | None:
    if not settings.DAGSTER_INGRESS_HOST:
        return None
    return f"https://{settings.DAGSTER_INGRESS_HOST}/runs/{run_id}"


def _table_name(step_key: str) -> str:
    # asset step keys are "<group>__<table>", e.g. "incremental__all_ping_hourly"
    return step_key.split("__")[-1]


def _user_error(error: SerializableErrorInfo) -> SerializableErrorInfo:
    # user code errors are wrapped in DagsterExecutionStepExecutionError
    if error.cls_name == "DagsterExecutionStepExecutionError" and error.cause:
        return error.cause
    return error


def _short_error(message: str) -> str:
    first_line = message.strip().split("\n")[0]
    if len(first_line) > MAX_ERROR_LENGTH:
        return f"{first_line[:MAX_ERROR_LENGTH]}..."
    return first_line


def _report_failure(
    context: RunFailureSensorContext,
    table: str | None,
    error_type: str,
    error_message: str,
    error_details: str,
) -> None:
    run = context.dagster_run
    if table:
        message = f"Analytics table failed: {table} ({run.job_name})"
    else:
        message = f"Analytics run failed: {run.job_name}"
    if error_message:
        message += f": {_short_error(error_message)}"

    context.log.error(message)
    event_id = sentry_sdk.capture_message(
        message,
        level="error",
        # one Sentry issue per table and error type, not per run
        fingerprint=[
            "analytics-tables-run-failure",
            run.job_name,
            table or "run",
            error_type,
        ],
        tags={
            "dagster.job": run.job_name,
            "dagster.table": table or "run",
            "dagster.run_id": run.run_id,
        },
        extras={"run_url": _run_url(run.run_id), "error": error_details},
    )
    context.log.info(f"Sentry event ID: {event_id}")


@run_failure_sensor(
    monitored_jobs=[analytics_tables_incremental_job, analytics_tables_daily_job],
    default_status=DefaultSensorStatus.RUNNING,
)
def analytics_tables__run_failure_sensor(context: RunFailureSensorContext):
    step_failures = context.get_step_failure_events()
    for event in step_failures:
        table = _table_name(event.step_key)
        error = event.event_specific_data.error
        if error is None:
            _report_failure(context, table, "Unknown", "", "")
        else:
            error = _user_error(error)
            _report_failure(
                context, table, error.cls_name, error.message, error.to_string()
            )

    if not step_failures:
        # the run failed outside a step, e.g. it hit dagster/max_runtime
        message = context.failure_event.message or ""
        _report_failure(context, None, "RunFailure", message, message)


def _format_duration(seconds: float) -> str:
    minutes = round(seconds / 60)
    if minutes < 60:
        return f"{minutes} min"
    return f"{minutes // 60} h {minutes % 60} min"


def _incremental_runs_summary(context: RunStatusSensorContext) -> str:
    records = context.instance.get_run_records(
        filters=RunsFilter(
            job_name=analytics_tables_incremental_job.name,
            created_after=datetime.now(UTC) - timedelta(hours=24),
        )
    )
    statuses = Counter(record.dagster_run.status for record in records)
    succeeded = statuses.pop(DagsterRunStatus.SUCCESS, 0)
    failed = statuses.pop(DagsterRunStatus.FAILURE, 0)
    others = sum(statuses.values())

    summary = f"Hourly tables, last 24 h: {succeeded} runs succeeded, {failed} failed"
    if others:
        summary += f", {others} cancelled or still running"
    return summary


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[analytics_tables_daily_job],
    default_status=DefaultSensorStatus.RUNNING,
)
def analytics_tables__daily_success_sensor(context: RunStatusSensorContext):
    run_id = context.dagster_run.run_id
    tables = len(
        context.instance.get_records_for_run(
            run_id=run_id, of_type=DagsterEventType.ASSET_MATERIALIZATION
        ).records
    )
    stats = context.instance.get_run_stats(run_id)

    text = f":white_check_mark: All {tables} daily analytics tables built"
    if stats.start_time and stats.end_time:
        text += f" in {_format_duration(stats.end_time - stats.start_time)}"
    if run_url := _run_url(run_id):
        text += f" (<{run_url}|run>)"
    text += f".\n{_incremental_runs_summary(context)}."
    if settings.DEPLOY_ENV != DeploymentEnvironment.PRODUCTION:
        text = f"[{settings.DEPLOY_ENV.name.lower()}] {text}"

    context.log.info(text)
    # unset pipeline variables reach the pod as the literal "$(NAME)"
    if not settings.SLACK_STATUS_WEBHOOK.startswith("https://"):
        context.log.info("SLACK_STATUS_WEBHOOK is not set, skipping Slack message")
        return
    asyncio.run(send_slack_base(text, webhook_url=settings.SLACK_STATUS_WEBHOOK))
