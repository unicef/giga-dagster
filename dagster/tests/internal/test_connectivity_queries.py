from unittest.mock import MagicMock, patch

from src.internal.connectivity_queries import (
    get_giga_meter_schools,
    get_mlab_schools,
    get_qos_tables,
    get_rt_schools,
)


def test_get_qos_tables():
    with patch("src.utils.db.trino.get_db_context") as mock_db:
        mock_conn = MagicMock()
        mock_db.return_value.__enter__.return_value = mock_conn

        mock_result = MagicMock()
        mock_result.mappings.return_value.all.return_value = [
            {"Table": "table_a"},
            {"Table": "table_b"},
            {"Table": "transforms"},
        ]
        mock_conn.execute.return_value = mock_result

        df = get_qos_tables()

        assert len(df) == 2
        assert "table_a" in df["Table"].values
        assert "transforms" not in df["Table"].values


def test_get_rt_schools():
    with patch("src.utils.db.gigamaps.get_db_context") as mock_db:
        mock_conn = MagicMock()
        mock_db.return_value.__enter__.return_value = mock_conn

        mock_result = MagicMock()
        mock_result.mappings.return_value.all.return_value = [
            {"school_id_giga": "1", "country_code": "BRA"}
        ]
        mock_conn.execute.return_value = mock_result

        df = get_rt_schools("BRA", is_test=True)
        assert len(df) == 1
        assert df.iloc[0]["school_id_giga"] == "1"


def test_get_mlab_schools():
    with patch("src.utils.db.gigameter.get_db_context") as mock_db:
        mock_conn = MagicMock()
        mock_db.return_value.__enter__.return_value = mock_conn

        mock_result = MagicMock()
        mock_result.mappings.return_value.all.return_value = []
        mock_conn.execute.return_value = mock_result

        df = get_mlab_schools("BRA", is_test=True)
        assert df.empty


def test_get_giga_meter_schools():
    with patch("src.utils.db.gigameter.get_db_context") as mock_db:
        mock_conn = MagicMock()
        mock_db.return_value.__enter__.return_value = mock_conn

        mock_result = MagicMock()
        mock_result.mappings.return_value.all.return_value = []
        mock_conn.execute.return_value = mock_result

        df = get_giga_meter_schools(is_test=True)
        assert df.empty
