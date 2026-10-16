GROUP_NAME = "facility_geolocation_data"

from src.assets.facility_geolocation.factory import (  # noqa: E402
    build_facility_geolocation_assets,
)

__all__ = ["GROUP_NAME", "build_facility_geolocation_assets"]
