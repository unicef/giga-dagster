from collections.abc import Sequence
from types import ModuleType

from dagster import ScheduleDefinition
from dagster._core.definitions.load_assets_from_modules import find_modules_in_package
from dagster._core.definitions.partitioned_schedule import (
    UnresolvedPartitionedAssetScheduleDefinition,
)

from .base import _find_definition_in_module


def load_schedules_from_package_module(
    package_module: ModuleType,
) -> Sequence[ScheduleDefinition | UnresolvedPartitionedAssetScheduleDefinition]:
    schedule_ids: set[int] = set()
    schedules: Sequence[
        ScheduleDefinition | UnresolvedPartitionedAssetScheduleDefinition
    ] = []
    for module in find_modules_in_package(package_module):
        for schedule in _find_definition_in_module(
            module,
            (ScheduleDefinition, UnresolvedPartitionedAssetScheduleDefinition),
        ):
            if schedule_id := id(schedule) not in schedule_ids:
                schedule_ids.add(schedule_id)
                schedules.append(schedule)
    return schedules
