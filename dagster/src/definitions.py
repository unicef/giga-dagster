from dagster import Definitions, load_assets_from_package_module
from src import jobs, sensors
from src.assets import (
    adhoc,
    admin,
    common,
    datahub_assets,
    migrations,
    school_coverage,
    school_geolocation,
)
from src.resources import RESOURCE_DEFINITIONS
from src.utils.load_module import (
    load_jobs_from_package_module,
    load_sensors_from_package_module,
)
from src.utils.sentry import setup_sentry

setup_sentry()


defs = Definitions(
    assets=[
        *load_assets_from_package_module(
            package_module=school_geolocation, group_name="school_geolocation_data"
        ),
        *load_assets_from_package_module(
            package_module=school_coverage, group_name="school_coverage_data"
        ),
        *load_assets_from_package_module(package_module=common, group_name="common"),
        *load_assets_from_package_module(
            package_module=datahub_assets, group_name="datahub"
        ),
        *load_assets_from_package_module(
            package_module=migrations, group_name="migrations"
        ),
        *load_assets_from_package_module(package_module=adhoc, group_name="adhoc"),
        *load_assets_from_package_module(package_module=admin, group_name="admin"),
    ],
    resources=RESOURCE_DEFINITIONS,
    jobs=load_jobs_from_package_module(jobs),
    sensors=load_sensors_from_package_module(sensors),
)
