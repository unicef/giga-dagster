from src.assets.facility_geolocation.factory import build_facility_geolocation_assets
from src.assets.facility_geolocation.publish_factory import (
    build_facility_publish_assets,
)
from src.facility_profiles import load_all_facility_specs

_SPECS = load_all_facility_specs()

FACILITY_GEOLOCATION_ASSETS = []
SCHOOL_FACILITY_GEOLOCATION_ASSETS = []
HEALTH_FACILITY_GEOLOCATION_ASSETS = []

for _schema_key, _spec in _SPECS.items():
    built = [
        *build_facility_geolocation_assets(_spec),
        *build_facility_publish_assets(_spec),
    ]
    FACILITY_GEOLOCATION_ASSETS.extend(built)
    if _schema_key == "schools":
        SCHOOL_FACILITY_GEOLOCATION_ASSETS.extend(built)
    elif _schema_key == "health_facilities":
        HEALTH_FACILITY_GEOLOCATION_ASSETS.extend(built)
