import numpy as np
import pandas as pd
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
from loguru import logger
from shapely.geometry import Point
from shapely.ops import nearest_points


def get_point(longitude: float, latitude: float) -> None | Point:
    try:
        return Point(longitude, latitude)
    except Exception as exc:
        logger.error(exc)
    return None


# Inside based on GADM admin boundaries data
def is_within_country_gadm(
    latitude: float,
    longitude: float,
    geometry: pd.Series | pd.DataFrame | np.ndarray,
) -> bool:
    point = get_point(longitude, latitude)
    if point is not None and geometry is not None:
        return point.within(geometry)

    return False


# Inside based on geopy
def is_within_country_geopy(
    latitude: float,
    longitude: float,
    country_code_iso2: str,
) -> bool:
    geolocator = Nominatim(user_agent="unicef_giga")
    coords = f"{latitude},{longitude}"

    if latitude is None or longitude is None:
        out = False
    else:
        location = geolocator.reverse(coords, timeout=10)
        geopy_country_code_iso2 = location.raw.get("address", {}).get(
            "country_code",
            "",
        )
        out = geopy_country_code_iso2.lower() == country_code_iso2.lower()

    return out


BOUNDARY_DISTANCE_THRESHOLD_KM = 1.5


# Inside based on boundary distance
def is_within_boundary_distance(
    latitude: float,
    longitude: float,
    geometry: pd.Series | pd.DataFrame | np.ndarray,
) -> bool:
    point = get_point(longitude, latitude)

    if point is not None and geometry is not None:
        p1, _ = nearest_points(geometry, point)

        # geodesic distance format is in lat-long, we need to switch these over!
        point1 = p1.coords[0]
        point1 = (point1[1], point1[0])
        point2 = (latitude, longitude)
        distance = geodesic(point1, point2).km

        return distance <= BOUNDARY_DISTANCE_THRESHOLD_KM

    return False


def boundary_distance(
    latitude: float,
    longitude: float,
    geometry: pd.Series | pd.DataFrame | np.ndarray,
) -> bool:
    point = get_point(longitude, latitude)

    if point is not None and geometry is not None:
        p1, _ = nearest_points(geometry, point)

        # geodesic distance format is in lat-long, we need to switch these over!
        point1 = p1.coords[0]
        point1 = (point1[1], point1[0])
        point2 = (latitude, longitude)
        distance = geodesic(point1, point2).km

    else:
        distance = None

    return distance


if __name__ == "__main__":
    latitude = 19.045252
    longitude = 124.408070

    from src.data_quality_checks.geography import get_country_geometry

    geometry = get_country_geometry("PHL")
    point = get_point(longitude, latitude)  # this is outside the country
    p1, _ = nearest_points(geometry, point)

    # geodesic distance format is in lat-long, we need to switch these over!
    point1 = p1.coords[0]
    point1 = (point1[1], point1[0])
    point2 = (latitude, longitude)
    distance = geodesic(point1, point2).km

    print(point1)
    print(point2)
    print(distance)
