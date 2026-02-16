from unittest.mock import MagicMock, patch

from src.internal.common_assets.master_release_notes import (
    aggregate_changes_by_column_and_type,
    send_master_release_notes,
)


def test_aggregate_changes_by_column_and_type(spark_session):
    data = [
        {"school_id_giga": "1", "_change_type": "insert", "col1": "A"},
        {"school_id_giga": "2", "_change_type": "update_preimage", "col1": "B"},
        {"school_id_giga": "2", "_change_type": "update_postimage", "col1": "C"},
    ]
    cdf = spark_session.createDataFrame(data)

    result = aggregate_changes_by_column_and_type(cdf)
    rows = result.collect()
    ops = {r["operation"] for r in rows}
    assert "insert" in ops
    assert "update" in ops


@patch("src.internal.common_assets.master_release_notes.DeltaTable")
async def test_send_master_release_notes_empty(mock_dt, spark_session):
    context = MagicMock()
    config = MagicMock()
    gold = MagicMock()
    gold.count.return_value = 0

    result = await send_master_release_notes(context, config, MagicMock(), gold)
    assert result is None
    context.log.warning.assert_called_with("No data in master, skipping email.")
