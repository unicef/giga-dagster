from collections.abc import Sequence
from types import ModuleType

from dagster import SensorDefinition
from dagster._core.definitions.load_assets_from_modules import _find_modules_in_package

from .base import _find_definition_in_module


def load_sensors_from_package_module(
    package_module: ModuleType,
) -> Sequence[SensorDefinition]:
    sensor_ids: set[int] = set()
    sensors: Sequence[SensorDefinition] = []
    for module in _find_modules_in_package(package_module):
        for sensor in _find_definition_in_module(module, SensorDefinition):
            if sensor_id := id(sensor) not in sensor_ids:
                sensor_ids.add(sensor_id)
                sensors.append(sensor)
    return sensors
