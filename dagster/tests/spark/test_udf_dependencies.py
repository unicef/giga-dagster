from unittest.mock import MagicMock, patch

import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon
from src.spark.udf_dependencies import (
    boundary_distance,
    get_point,
    is_within_boundary_distance,
    is_within_country_gadm,
    is_within_country_geopy,
    is_within_country_mapbox,
)


def test_get_point():
    p = get_point(10.0, 20.0)
    assert p.x == 10.0
    assert p.y == 20.0
    p_inv = get_point(None, None)
    assert p_inv.x == 181
    assert p_inv.y == 91


def test_is_within_country_gadm():
    poly = Polygon([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)])
    boundaries = gpd.GeoDataFrame({"GID_0": ["TST"]}, geometry=[poly], crs="epsg:4326")
    lats = pd.Series([5.0, 15.0])
    lons = pd.Series([5.0, 5.0])
    result = is_within_country_gadm(lats, lons, boundaries, "TST")
    assert result[0] == 0
    assert result[1] == 1


def test_is_within_country_mapbox():
    poly = Polygon([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)])
    boundaries = gpd.GeoDataFrame(
        {"iso_3166_1_alpha_3": ["TST"]}, geometry=[poly], crs="epsg:4326"
    )
    lats = pd.Series([5.0, 15.0])
    lons = pd.Series([5.0, 5.0])
    result = is_within_country_mapbox(lats, lons, boundaries)
    assert result[0] == 0
    assert result[1] == 1


@patch("src.spark.udf_dependencies.Nominatim")
def test_is_within_country_geopy(MockNominatim):
    mock_geo = MockNominatim.return_value
    mock_location = MagicMock()
    mock_location.raw = {"address": {"country_code": "ph"}}
    mock_geo.reverse.return_value = mock_location
    assert is_within_country_geopy(14.0, 121.0, "PH") is True
    mock_location.raw = {"address": {"country_code": "us"}}
    assert is_within_country_geopy(14.0, 121.0, "PH") is False
    assert is_within_country_geopy(None, None, "PH") is False
    mock_geo.reverse.side_effect = ValueError("Error")
    assert is_within_country_geopy(14.0, 121.0, "PH") is False


def test_within_boundary_distance():
    poly = Polygon([(0, 0), (0.01, 0), (0.01, 0.01), (0, 0.01), (0, 0)])
    assert is_within_boundary_distance(0.005, 0.005, poly, 1) is True
    assert is_within_boundary_distance(1.0, 1.0, poly, 1) is False
    assert is_within_boundary_distance("a", "b", poly, 1) is False
    assert is_within_boundary_distance(0, 0, None, 1) is False


def test_boundary_distance():
    poly = Polygon([(0, 0), (0.01, 0), (0.01, 0.01), (0, 0.01), (0, 0)])
    dist = boundary_distance(1.0, 1.0, poly)
    assert dist > 1.5
    dist_near = boundary_distance(0.011, 0.0, poly)
    assert dist_near < 1.0
    assert boundary_distance("a", "b", poly) == 1000
