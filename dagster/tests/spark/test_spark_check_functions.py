from unittest.mock import MagicMock, patch

import pandas as pd

with patch.dict(
    "os.environ",
    {
        "AZURE_SAS_TOKEN": "mock_token",
        "AZURE_BLOB_CONTAINER_NAME": "mock_container",
        "GIGAMAPS_DB_CONNECTION_STRING": "postgresql://dummy:5432/db",
        "GIGAMETER_DB_CONNECTION_STRING": "postgresql://dummy:5432/db",
    },
):
    pass

from src.spark.check_functions import (
    are_pair_points_beyond_minimum_distance,
    duplicate_check,
    get_country_geometry,
    get_decimal_places,
    has_at_least_n_decimal_places,
    has_same_availability,
    has_similar_name,
    has_value,
    is_available,
    is_same_name_level_within_radius,
    is_valid_range,
    is_within_country_gadm,
    is_within_country_geopy,
)


def test_is_valid_range():
    assert is_valid_range(5, 1, 10) is True
    assert is_valid_range(0, 1, 10) is False
    assert is_valid_range(11, 1, 10) is False

    assert is_valid_range(5, 1, None) is True
    assert is_valid_range(0, 1, None) is False

    assert is_valid_range(5, None, 10) is True
    assert is_valid_range(11, None, 10) is False

    assert is_valid_range("string", 1, 10) is False


def test_is_available():
    assert is_available("Yes") is True
    assert is_available("yes") is True
    assert is_available("YES ") is True
    assert is_available("No") is False
    assert is_available(None) is False


def test_has_value():
    assert has_value("value") is True
    assert has_value(1) is True
    assert has_value(None) is False
    assert has_value("") is False


def test_has_same_availability():
    assert has_same_availability("Yes", "value") is True
    assert has_same_availability("No", None) is True
    assert has_same_availability("Yes", None) is False
    assert has_same_availability("No", "value") is False


def test_get_decimal_places():
    assert get_decimal_places(1.23) == 2
    assert get_decimal_places(10.5) == 1
    assert get_decimal_places(100) == 0
    assert get_decimal_places(None) is None


def test_has_at_least_n_decimal_places():
    assert has_at_least_n_decimal_places(1.2345, 4) is True
    assert has_at_least_n_decimal_places(1.23, 4) is False


def test_are_pair_points_beyond_minimum_distance():
    pt1 = (10.0, 10.0)
    pt2 = (11.0, 11.0)

    assert are_pair_points_beyond_minimum_distance(pt1, pt2) is False

    pt3 = (10.00001, 10.00001)
    assert are_pair_points_beyond_minimum_distance(pt1, pt3) is True


def test_has_similar_name():
    with patch("src.spark.check_functions.config") as mock_conf:
        mock_conf.SIMILARITY_RATIO_CUTOFF = 0.8

        assert has_similar_name("School A", ["Academy B", "School A"]) is False

        assert has_similar_name("School Alpha", ["School Al pha"]) is True

        assert has_similar_name("Apple", ["Banana"]) is False


def test_is_same_name_level_within_radius():
    row1 = {
        "school_name": "A",
        "education_level": "Primary",
        "latitude": 10.0,
        "longitude": 10.0,
    }

    row2 = {
        "school_name": "A",
        "education_level": "Primary",
        "latitude": 10.00001,
        "longitude": 10.00001,
    }
    assert is_same_name_level_within_radius(row1, row2) is False

    row3 = {
        "school_name": "A",
        "education_level": "Primary",
        "latitude": 11.0,
        "longitude": 11.0,
    }

    assert is_same_name_level_within_radius(row1, row3) is True
    assert is_same_name_level_within_radius(row1, row2) is False


def test_get_country_geometry():
    with patch("src.spark.check_functions.gpd.read_file") as mock_read_file:
        mock_gdf = MagicMock()
        mock_gdf.__getitem__.return_value.__getitem__.return_value.__getitem__.return_value = "GeometryObject"

        mock_read_file.return_value = mock_gdf

        geo = get_country_geometry("USA")
        assert geo == "GeometryObject"


@patch("src.spark.check_functions.get_country_geometry")
@patch("src.spark.check_functions.get_point")
def test_is_within_country_gadm(mock_get_point, mock_get_geo):
    mock_point = MagicMock()
    mock_geo = MagicMock()

    mock_get_point.return_value = mock_point
    mock_get_geo.return_value = mock_geo

    mock_point.within.return_value = True

    assert is_within_country_gadm(10, 10, "USA") is True


@patch("src.spark.check_functions.Nominatim")
@patch("src.spark.check_functions.coco.convert")
def test_is_within_country_geopy(mock_coco, mock_nominatim):
    mock_coco.return_value = "US"

    geolocator = mock_nominatim.return_value
    location = MagicMock()
    location.raw = {"address": {"country_code": "us"}}
    geolocator.reverse.return_value = location

    assert is_within_country_geopy(10, 10, "USA") is True

    location.raw = {"address": {"country_code": "ca"}}
    assert is_within_country_geopy(10, 10, "USA") is False

    geolocator.reverse.return_value = None
    assert is_within_country_geopy(10, 10, "USA") is False


def test_duplicate_check():
    df = pd.DataFrame(
        {
            "school_name": ["A", "A", "B"],
            "education_level": [1, 1, 2],
            "latitude": [10.0, 10.00001, 12.0],
            "longitude": [10.0, 10.00001, 12.0],
        }
    )

    def simple_check(r1, r2):
        return r1["school_name"] == r2["school_name"]

    dupes = duplicate_check(df, simple_check)

    assert dupes[0] is True
    assert dupes[1] is True
    assert dupes[2] is False
