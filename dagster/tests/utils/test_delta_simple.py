from unittest.mock import MagicMock

from src.utils.delta import build_deduped_merge_query


def test_build_deduped_merge_query_working(spark_session):
    mock_master_table = MagicMock()
    mock_master_df = MagicMock()
    mock_master_table.toDF.return_value = mock_master_df

    mock_updates_df = MagicMock()
    mock_incoming_df = MagicMock()
    mock_updates_df.alias.return_value = mock_incoming_df

    mock_incoming_ids = MagicMock()
    mock_incoming_df.select.return_value = mock_incoming_ids

    mock_master_ids = MagicMock()
    mock_master_df.select.return_value = mock_master_ids

    mock_updates_join = MagicMock()
    mock_incoming_ids.join.return_value = mock_updates_join

    mock_generic_joined = MagicMock()
    mock_incoming_ids.join.return_value = mock_generic_joined
    mock_master_ids.join.return_value = mock_generic_joined

    mock_generic_joined.filter.return_value = mock_generic_joined
    mock_generic_joined.limit.return_value = mock_generic_joined
    mock_generic_joined.count.return_value = 1

    mock_merge_builder = MagicMock()
    mock_master_table.alias.return_value.merge.return_value = mock_merge_builder

    res = build_deduped_merge_query(
        master=mock_master_table,
        updates=mock_updates_df,
        primary_key="id",
        update_columns=["col1"],
        context=MagicMock(),
    )

    if res is None:
        pass

    assert res is not None
