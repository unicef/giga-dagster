from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

try:
    import geopandas as gpd
    from shapely.geometry import Polygon
except ImportError:
    gpd = None

from src.spark.transform_functions import (
    connectivity_rt_dataset,
    merge_connectivity_to_master,
)


@pytest.fixture
def mock_admin_boundaries():
    if not gpd:
        return None
    poly = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    gdf = gpd.GeoDataFrame(
        {
            "name": ["TestNative"],
            "name_en": ["TestEnglish"],
            "admin1_id_giga": ["GID_1"],
            "geometry": [poly],
        },
        crs="EPSG:4326",
    )
    return gdf


@pytest.fixture
def mock_disputed_boundaries():
    if not gpd:
        return None
    poly = Polygon([(0, 0), (5, 0), (5, 5), (0, 5)])
    gdf = gpd.GeoDataFrame(
        {"name": ["DisputedRegion"], "geometry": [poly]}, crs="EPSG:4326"
    )
    return gdf


def test_connectivity_rt_dataset(spark_session):
    rt_data = [
        {
            "connectivity_rt_ingestion_timestamp": None,
            "country": "TestCountry",
            "country_code": "TC",
            "school_id_giga": "S1",
            "school_id_govt": "G1",
        }
    ]
    mlab_data = [
        {
            "country_code": "TC",
            "mlab_created_date": "2023-01-01",
            "school_id_govt": "G1",
            "source": "MLab",
        }
    ]
    dca_data = [{"school_id_giga": "S1", "school_id_govt": "G1", "source": "PCDC"}]

    with (
        patch(
            "src.internal.connectivity_queries.get_rt_schools",
            return_value=pd.DataFrame(rt_data),
        ),
        patch(
            "src.internal.connectivity_queries.get_mlab_schools",
            return_value=pd.DataFrame(mlab_data),
        ),
        patch(
            "src.internal.connectivity_queries.get_giga_meter_schools",
            return_value=pd.DataFrame(dca_data),
        ),
    ):
        result_df = connectivity_rt_dataset(
            spark=spark_session,
            iso2_country_code="TC",
            is_test=True,
            context=MagicMock(),
        )

        rows = result_df.collect()
        assert len(rows) == 1
        assert rows[0]["school_id_giga"] == "S1"
        assert "PCDC" in rows[0]["connectivity_RT_datasource"]
        assert "MLab" in rows[0]["connectivity_RT_datasource"]


def test_merge_connectivity_to_master(spark_session):
    master_data = [
        {
            "school_id_govt": "G1",
            "connectivity_RT": "No",
            "connectivity_govt": "No",
            "download_speed_govt": 0.0,
            "school_id_giga": "S1",
        }
    ]
    master_df = spark_session.createDataFrame(master_data)

    conn_data = [
        {
            "school_id_govt": "G1",
            "school_id_giga": "S1",
            "connectivity_RT": "Yes",
            "connectivity_govt": "Yes",
            "download_speed_govt": 10.0,
        }
    ]
    conn_df = spark_session.createDataFrame(conn_data)

    uploaded_columns = ["download_speed_govt", "connectivity_govt"]
    mode = "create"

    result_df = merge_connectivity_to_master(master_df, conn_df, uploaded_columns, mode)

    row = result_df.collect()[0]

    assert row["connectivity_RT"] == "Yes"
    assert row["connectivity"] == "Yes"
