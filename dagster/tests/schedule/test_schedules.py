from src.schedule.datahub import (
    datahub_materialize_prerequisities_schedule,
    datahub_update_access_schedule,
)
from src.schedule.qos_availability import superset_schedule as qos_schedule
from src.schedule.school_connectivity import (
    school_connectivity__get_new_realtime_schools_schedule,
)
from src.schedule.superset import superset_schedule as superset_sch

from dagster import ScheduleDefinition


def test_schedules_exist():
    assert isinstance(datahub_materialize_prerequisities_schedule, ScheduleDefinition)
    assert datahub_materialize_prerequisities_schedule.cron_schedule == "0 0 1 1 *"

    assert isinstance(datahub_update_access_schedule, ScheduleDefinition)
    assert datahub_update_access_schedule.cron_schedule == "30 * * * *"

    assert isinstance(qos_schedule, ScheduleDefinition)
    assert qos_schedule.cron_schedule == "10 * * * *"

    assert isinstance(
        school_connectivity__get_new_realtime_schools_schedule, ScheduleDefinition
    )
    assert (
        school_connectivity__get_new_realtime_schools_schedule.cron_schedule
        == "0 0 * * *"
    )

    assert isinstance(superset_sch, ScheduleDefinition)
    assert superset_sch.cron_schedule == "15 3 * * *"
