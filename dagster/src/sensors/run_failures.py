import sentry_sdk

from dagster import RunFailureSensorContext, run_failure_sensor
from src.utils.sentry import SENTRY_ENABLED


@run_failure_sensor(name="run_failures__sentry_sensor")
def run_failures__sentry_sensor(context: RunFailureSensorContext):
    # Op failures are already captured by @capture_op_exceptions
    if not SENTRY_ENABLED or context.get_step_failure_events():
        return

    run = context.dagster_run
    message = context.failure_event.message or "Run failed without a step failure"

    with sentry_sdk.push_scope() as scope:
        scope.set_tag("job_name", run.job_name)
        scope.set_tag("run_id", run.run_id)
        scope.set_context("dagster_run", {"tags": dict(run.tags)})
        scope.fingerprint = ["dagster-run-failure", run.job_name]
        event_id = sentry_sdk.capture_message(message, level="error")

    context.log.info(f"Sentry captured run failure. Event ID: {event_id}")
