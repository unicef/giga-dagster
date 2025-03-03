from dagster import Definitions, load_assets_from_package_module
from src import jobs, schedule, sensors
from src.assets import (
    adhoc,
    admin,
    common,
    datahub_assets,
    debug,
    migrations,
    school_connectivity,
    school_coverage,
    school_geolocation,
    school_list,
    unstructured,
)
from src.resources import RESOURCE_DEFINITIONS
from src.utils.load_module import (
    load_jobs_from_package_module,
    load_schedules_from_package_module,
    load_sensors_from_package_module,
)
from src.utils.sentry import setup_sentry


setup_sentry()


defs = Definitions(
    assets=[
        *load_assets_from_package_module(
            package_module=school_geolocation,
            group_name=school_geolocation.GROUP_NAME,
        ),
        *load_assets_from_package_module(
            package_module=school_coverage,
            group_name=school_coverage.GROUP_NAME,
        ),
        *load_assets_from_package_module(
            package_module=school_list,
            group_name=school_list.GROUP_NAME,
        ),
        *load_assets_from_package_module(
            package_module=school_connectivity,
            group_name=school_connectivity.GROUP_NAME,
        ),
        *load_assets_from_package_module(
            package_module=common, group_name=common.GROUP_NAME
        ),
        *load_assets_from_package_module(
            package_module=datahub_assets,
            group_name=datahub_assets.GROUP_NAME,
        ),
        *load_assets_from_package_module(
            package_module=migrations,
            group_name=migrations.GROUP_NAME,
        ),
        *load_assets_from_package_module(
            package_module=adhoc,
            group_name=adhoc.GROUP_NAME,
        ),
        *load_assets_from_package_module(
            package_module=admin,
            group_name=admin.GROUP_NAME,
        ),
        *load_assets_from_package_module(
            package_module=debug,
            group_name=debug.GROUP_NAME,
        ),
        *load_assets_from_package_module(
            package_module=unstructured,
            group_name=unstructured.GROUP_NAME,
        ),
    ],
    resources=RESOURCE_DEFINITIONS,
    jobs=load_jobs_from_package_module(jobs),
    schedules=load_schedules_from_package_module(schedule),
    sensors=load_sensors_from_package_module(sensors),
)
