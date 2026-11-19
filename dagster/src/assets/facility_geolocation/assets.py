from src.assets.facility_geolocation.factory import build_facility_geolocation_assets
from src.assets.facility_geolocation.publish_factory import (
    build_facility_publish_assets,
)
from src.facilities import HEALTH_SPEC, SCHOOL_SPEC

SCHOOL_FACILITY_GEOLOCATION_ASSETS = [
    *build_facility_geolocation_assets(SCHOOL_SPEC),
    *build_facility_publish_assets(SCHOOL_SPEC),
]

HEALTH_FACILITY_GEOLOCATION_ASSETS = [
    *build_facility_geolocation_assets(HEALTH_SPEC),
    *build_facility_publish_assets(HEALTH_SPEC),
]

FACILITY_GEOLOCATION_ASSETS = [
    *SCHOOL_FACILITY_GEOLOCATION_ASSETS,
    *HEALTH_FACILITY_GEOLOCATION_ASSETS,
]
