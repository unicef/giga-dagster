"""Run status sensors for the analytics tables jobs.

Small stand-in jobs with the real job names are executed in process, so the
sensors read the same run records and events they get in production.
"""

import pytest
import sentry_sdk
from sentry_sdk.transport import Transport
from src.sensors import analytics_tables as sensors
from src.settings import DeploymentEnvironment, settings
from src.utils import sentry

from dagster import (
    AssetSelection,
    DagsterEventType,
    DagsterInstance,
    DagsterRunStatus,
    Definitions,
    asset,
    build_run_status_sensor_context,
    define_asset_job,
)
from dagster._core.storage.dagster_run import DagsterRun

INCREMENTAL_JOB = "analytics_tables__incremental_job"
DAILY_JOB = "analytics_tables__daily_job"


class CapturingTransport(Transport):
    def __init__(self):
        super().__init__()
        self.events = []

    def capture_envelope(self, envelope):
        self.events.extend(item.payload.json for item in envelope.items)


@pytest.fixture
def deploy_env():
    # DEPLOY_ENV as the Azure pipelines set it
    return "prd"


@pytest.fixture
def sentry_events(monkeypatch, deploy_env):
    # production setup_sentry(), with events kept in memory instead of sent
    transport = CapturingTransport()
    init = sentry_sdk.init
    monkeypatch.setattr(
        sentry_sdk, "init", lambda **kwargs: init(transport=transport, **kwargs)
    )
    monkeypatch.setattr(settings, "DEPLOY_ENV", DeploymentEnvironment(deploy_env))
    monkeypatch.setattr(sentry, "SENTRY_ENABLED", True)
    monkeypatch.setattr(settings, "DAGSTER_SENTRY_DSN", "https://key@sentry.test/1")
    sentry.setup_sentry()
    yield transport.events
    init()


@pytest.fixture
def instance():
    with DagsterInstance.ephemeral() as instance:
        yield instance


@pytest.fixture
def slack_messages(monkeypatch):
    messages = []

    async def fake_send_slack_base(text, context=None, webhook_url=""):
        messages.append((webhook_url, text))

    monkeypatch.setattr(sensors, "send_slack_base", fake_send_slack_base)
    monkeypatch.setattr(settings, "SLACK_STATUS_WEBHOOK", "https://hooks.slack.test/x")
    monkeypatch.setattr(settings, "DAGSTER_INGRESS_HOST", "dagster.example.org")
    monkeypatch.setattr(settings, "DEPLOY_ENV", DeploymentEnvironment.PRODUCTION)
    return messages


@asset(key_prefix=["incremental"], group_name="incremental")
def ok_table() -> None:
    pass


@asset(key_prefix=["incremental"], group_name="incremental", deps=[ok_table])
def broken_table() -> None:
    raise ValueError("TABLE_NOT_FOUND: Table 'default.missing' does not exist")


@asset(key_prefix=["daily"], group_name="daily")
def daily_one() -> None:
    pass


@asset(key_prefix=["daily"], group_name="daily")
def daily_two() -> None:
    pass


defs = Definitions(
    assets=[ok_table, broken_table, daily_one, daily_two],
    jobs=[
        define_asset_job(
            INCREMENTAL_JOB, selection=AssetSelection.groups("incremental")
        ),
        define_asset_job(DAILY_JOB, selection=AssetSelection.groups("daily")),
    ],
)


def _run(instance: DagsterInstance, job_name: str) -> DagsterRun:
    result = defs.get_job_def(job_name).execute_in_process(
        instance=instance, raise_on_error=False
    )
    return instance.get_run_by_id(result.run_id)


def _sensor_context(instance, dagster_run, event_type, sensor_name):
    record = instance.get_records_for_run(
        run_id=dagster_run.run_id, of_type=event_type
    ).records[-1]
    return build_run_status_sensor_context(
        sensor_name=sensor_name,
        dagster_event=record.event_log_entry.dagster_event,
        dagster_instance=instance,
        dagster_run=dagster_run,
    )


def _add_runs(instance, job_name, statuses):
    for status in statuses:
        instance.add_run(DagsterRun(job_name=job_name, status=status))


class TestRunFailureSensor:
    @pytest.mark.parametrize(
        ("deploy_env", "environment"),
        [
            ("dev", "development"),
            ("stg", "staging"),
            ("prd", "production"),
        ],
    )
    def test_failed_table_is_sent_to_sentry(self, instance, sentry_events, environment):
        dagster_run = _run(instance, INCREMENTAL_JOB)
        assert dagster_run.status == DagsterRunStatus.FAILURE

        context = _sensor_context(
            instance,
            dagster_run,
            DagsterEventType.RUN_FAILURE,
            sensors.analytics_tables__run_failure_sensor.name,
        ).for_run_failure()
        sensors.analytics_tables__run_failure_sensor(context)

        assert len(sentry_events) == 1
        event = sentry_events[0]
        assert event["environment"] == environment
        assert event["level"] == "error"
        assert event["message"] == (
            "Analytics table failed: broken_table (analytics_tables__incremental_job): "
            "ValueError: TABLE_NOT_FOUND: Table 'default.missing' does not exist"
        )
        assert event["fingerprint"] == [
            "analytics-tables-run-failure",
            INCREMENTAL_JOB,
            "broken_table",
            "ValueError",
        ]
        assert event["tags"]["dagster.table"] == "broken_table"
        assert event["tags"]["dagster.run_id"] == dagster_run.run_id
        assert "Stack Trace:" in event["extra"]["error"]

    def test_run_failure_without_failed_step_is_sent_to_sentry(
        self, instance, sentry_events
    ):
        dagster_run = instance.add_run(DagsterRun(job_name=DAILY_JOB))
        instance.report_run_failed(
            dagster_run, message="Exceeded maximum runtime of 3600 seconds."
        )

        context = _sensor_context(
            instance,
            instance.get_run_by_id(dagster_run.run_id),
            DagsterEventType.RUN_FAILURE,
            sensors.analytics_tables__run_failure_sensor.name,
        ).for_run_failure()
        sensors.analytics_tables__run_failure_sensor(context)

        assert len(sentry_events) == 1
        assert sentry_events[0]["message"] == (
            "Analytics run failed: analytics_tables__daily_job: "
            "Exceeded maximum runtime of 3600 seconds."
        )
        assert sentry_events[0]["fingerprint"][2:] == ["run", "RunFailure"]


class TestDailySuccessSensor:
    def test_posts_summary_to_slack(self, instance, slack_messages):
        _add_runs(
            instance,
            INCREMENTAL_JOB,
            [DagsterRunStatus.SUCCESS] * 21
            + [DagsterRunStatus.FAILURE, DagsterRunStatus.STARTED],
        )
        _add_runs(instance, DAILY_JOB, [DagsterRunStatus.FAILURE])
        dagster_run = _run(instance, DAILY_JOB)

        context = _sensor_context(
            instance,
            dagster_run,
            DagsterEventType.RUN_SUCCESS,
            sensors.analytics_tables__daily_success_sensor.name,
        )
        sensors.analytics_tables__daily_success_sensor(context)

        assert slack_messages == [
            (
                "https://hooks.slack.test/x",
                ":white_check_mark: All 2 daily analytics tables built in 0 min "
                f"(<https://dagster.example.org/runs/{dagster_run.run_id}|run>).\n"
                "Hourly tables, last 24 h: 21 runs succeeded, 1 failed, "
                "1 cancelled or still running.",
            )
        ]

    @pytest.mark.parametrize(
        ("deploy_env", "label"),
        [
            ("dev", "[development]"),
            ("stg", "[staging]"),
        ],
    )
    def test_labels_non_production_environments(
        self, instance, slack_messages, monkeypatch, deploy_env, label
    ):
        monkeypatch.setattr(settings, "DEPLOY_ENV", DeploymentEnvironment(deploy_env))
        dagster_run = _run(instance, DAILY_JOB)

        context = _sensor_context(
            instance,
            dagster_run,
            DagsterEventType.RUN_SUCCESS,
            sensors.analytics_tables__daily_success_sensor.name,
        )
        sensors.analytics_tables__daily_success_sensor(context)

        assert slack_messages[0][1].startswith(f"{label} :white_check_mark:")
        assert slack_messages[0][1].endswith("0 runs succeeded, 0 failed.")

    @pytest.mark.parametrize("webhook", ["", "$(SLACK_STATUS_WEBHOOK)"])
    def test_skips_slack_without_webhook(
        self, instance, slack_messages, monkeypatch, webhook
    ):
        monkeypatch.setattr(settings, "SLACK_STATUS_WEBHOOK", webhook)
        dagster_run = _run(instance, DAILY_JOB)

        context = _sensor_context(
            instance,
            dagster_run,
            DagsterEventType.RUN_SUCCESS,
            sensors.analytics_tables__daily_success_sensor.name,
        )
        sensors.analytics_tables__daily_success_sensor(context)

        assert slack_messages == []
