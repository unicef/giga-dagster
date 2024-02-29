from geopy.distance import geodesic
from geopy.geocoders import Nominatim
from shapely.geometry import Point
from shapely.ops import nearest_points

from src.settings import settings


def get_point(longitude: float, latitude: float):
    try:
        point = Point(longitude, latitude)
    except:  # noqa: E722
        point = None

    return point


# Inside based on GADM admin boundaries data
def is_within_country_gadm(latitude: float, longitude: float, geometry) -> bool:
    point = get_point(longitude, latitude)
    if point is not None and geometry is not None:
        out = point.within(geometry)
    else:
        out = False
    return out


# Inside based on geopy
def is_within_country_geopy(
    latitude: float, longitude: float, country_code_iso2: str
) -> bool:
    geolocator = Nominatim(user_agent=f"GigaSyncDagster/{settings.COMMIT_SHA[:7]}")
    coords = f"{latitude},{longitude}"

    if latitude is None or longitude is None:
        out = False
    else:
        location = geolocator.reverse(coords, timeout=10)
        geopy_country_code_iso2 = location.raw.get("address", {}).get(
            "country_code", ""
        )
        out = geopy_country_code_iso2.lower() == country_code_iso2.lower()

    return out


# Inside based on boundary distance
def is_within_boundary_distance(latitude: float, longitude: float, geometry) -> bool:
    point = get_point(longitude, latitude)

    if point is not None and geometry is not None:
        p1, p2 = nearest_points(geometry, point)
        point1 = p1.coords[0]
        point2 = (longitude, latitude)
        distance = geodesic(point1, point2).km
        out = distance <= 1.5  # km
    else:
        out = False

    return out
