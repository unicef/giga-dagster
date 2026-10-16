from src.assets.facility_geolocation.factory import build_facility_geolocation_assets
from src.assets.facility_geolocation.publish_factory import (
    build_facility_publish_assets,
)
from src.facilities import HEALTH_SPEC

HEALTH_FACILITY_GEOLOCATION_ASSETS = [
    *build_facility_geolocation_assets(HEALTH_SPEC),
    *build_facility_publish_assets(HEALTH_SPEC),
]
