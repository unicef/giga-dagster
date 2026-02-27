from unittest.mock import MagicMock, patch

import geopandas as gpd
import pytest
from shapely.geometry import Polygon
from src.spark.check_functions import (
    are_pair_points_beyond_minimum_distance,
    get_country_geometry,
    get_decimal_places,
    get_point,
    has_similar_name,
    has_value,
    is_available,
    is_valid_range,
    is_within_boundary_distance,
    is_within_country_gadm,
    is_within_country_geopy,
)


@pytest.fixture
def mock_settings(monkeypatch):
    monkeypatch.setenv("AZURE_SAS_TOKEN", "fake_token")
    monkeypatch.setenv("AZURE_BLOB_CONTAINER_NAME", "fake_container")


def test_get_point():
    point = get_point(10.0, 20.0)
    assert point.x == 10.0
    assert point.y == 20.0


@patch("src.spark.check_functions.BlobServiceClient")
def test_get_country_geometry(mock_blob_service, mock_settings, spark_session):
    mock_client = MagicMock()
    mock_blob_service.return_value = mock_client
    mock_blob_client = MagicMock()
    mock_client.get_blob_client.return_value = mock_blob_client
    with patch("src.spark.check_functions.gpd.read_file") as mock_read_file:
        poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        mock_df = gpd.GeoDataFrame({"GID_0": ["BRA"], "geometry": [poly]})
        mock_read_file.return_value = mock_df
        geom = get_country_geometry("BRA")
        assert geom == poly


@patch("src.spark.check_functions.get_country_geometry")
def test_is_within_country_gadm(mock_get_geo):
    poly = Polygon([(0, 0), (0, 2), (2, 2), (2, 0)])
    mock_get_geo.return_value = poly
    assert is_within_country_gadm(1.0, 1.0, "BRA") is True
    assert is_within_country_gadm(3.0, 3.0, "BRA") is False
    mock_get_geo.return_value = None
    assert is_within_country_gadm(1.0, 1.0, "BRA") is None


@patch("src.spark.check_functions.Nominatim")
@patch("src.spark.check_functions.coco.convert")
def test_is_within_country_geopy(mock_coco_convert, mock_nominatim):
    mock_coco_convert.return_value = "BR"
    mock_geolocator = MagicMock()
    mock_nominatim.return_value = mock_geolocator
    mock_location = MagicMock()
    mock_location.raw = {"address": {"country_code": "br"}}
    mock_geolocator.reverse.return_value = mock_location
    assert is_within_country_geopy(1.0, 1.0, "BRA") is True
    mock_location.raw = {"address": {"country_code": "us"}}
    assert is_within_country_geopy(1.0, 1.0, "BRA") is False
    mock_geolocator.reverse.return_value = None
    assert is_within_country_geopy(1.0, 1.0, "BRA") is False


@patch("src.spark.check_functions.get_country_geometry")
def test_is_within_boundary_distance(mock_get_geo):
    poly = Polygon([(0, 0), (0, 2), (2, 2), (2, 0)])
    mock_get_geo.return_value = poly
    assert is_within_boundary_distance(0.0, 0.0, "BRA") is True
    assert is_within_boundary_distance(10.0, 10.0, "BRA") is False


def test_is_available():
    assert is_available("yes") is True
    assert is_available("Yes") is True
    assert is_available("YES ") is True
    assert is_available("no") is False
    assert is_available(None) is False


def test_has_value():
    assert has_value("a") is True
    assert has_value("") is False
    assert has_value(None) is False


def test_get_decimal_places():
    assert get_decimal_places(1.1) == 1
    assert get_decimal_places(1.12) == 2
    assert get_decimal_places(1) == 0
    assert get_decimal_places(None) is None


def test_are_pair_points_beyond_minimum_distance():
    p1 = (10.0, 10.0)
    p2 = (10.0, 10.0)
    assert are_pair_points_beyond_minimum_distance(p1, p2) is True
    p3 = (11.0, 11.0)
    assert are_pair_points_beyond_minimum_distance(p1, p3) is False


def test_has_similar_name():
    name_list_distinct = ["Banana", "Cherry"]
    assert has_similar_name("Apple", name_list_distinct) is False
    name_list_sim = ["Apple"]
    assert has_similar_name("Apple1", name_list_sim) is True
    assert has_similar_name("Zeta", name_list_sim) is False


def test_is_valid_range():
    assert is_valid_range(5, 1, 10) is True
    assert is_valid_range(0, 1, 10) is False
    assert is_valid_range(11, 1, 10) is False
    assert is_valid_range(5, 1, None) is True
    assert is_valid_range(0, 1, None) is False
    assert is_valid_range(5, None, 10) is True
    assert is_valid_range(11, None, 10) is False
    assert is_valid_range("5", 1, 10) is False
