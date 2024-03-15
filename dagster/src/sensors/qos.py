from datetime import datetime

import croniter
from models.qos_apis import SchoolConnectivity
from sqlalchemy import _and, select

from dagster import RunConfig, RunRequest, sensor
from src.jobs.qos import qos_school_connectivity__automated_data_checks_job
from src.utils.db import get_db_context


@sensor(
    job=qos_school_connectivity__automated_data_checks_job,
    minimum_interval_seconds=60,
)
def qos_school_connectivity_sensor():
    timestamp = datetime.now()
    with get_db_context() as session:
        school_connectivity_apis = session.scalars(
            select(SchoolConnectivity).where(
                _and(
                    SchoolConnectivity.enabled,
                    croniter(SchoolConnectivity.cron_schedule).get_next() <= timestamp,
                )
            )
        )

    ops_list = [
        "qos_school_connectivity_raw",
        "qos_school_connectivity_bronze",
        "qos_school_connectivity_data_quality_results",
        "qos_school_connectivity_dq_passed_rows",
        "qos_school_connectivity_dq_failed_rows",
        "qos_school_connectivity_silver",
        "qos_school_connectivity_gold",
    ]

    for api in school_connectivity_apis:
        config = api.__dict__

        yield RunRequest(
            run_key=f"{config["name"]}_{timestamp.strftime("%Y-%m-%d")}",
            run_config=RunConfig(
                ops=dict(zip(ops_list, [config] * len(ops_list), strict=True))
            ),
        )
