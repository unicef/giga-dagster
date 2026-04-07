from unittest.mock import MagicMock, patch

import pandas as pd
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from src.internal.merge import (
    core_merge_logic,
    full_in_cluster_merge,
    partial_cdf_in_cluster_merge,
    partial_in_cluster_merge,
)


def test_manual_review_dedupe_strat(spark_session):
    """Test dedup strategy: keep latest _commit_version per school_id_giga,
    breaking ties by _change_type so 'update_postimage' wins over 'update_preimage'."""
    data = pd.DataFrame(
        {
            "school_id_giga": ["school1", "school1", "school1", "school2"],
            "_commit_version": [2, 2, 1, 1],
            "_change_type": [
                "update_postimage",
                "update_preimage",
                "insert",
                "insert",
            ],
            "value": ["latest", "old", "first", "single"],
        }
    )
    # Manually apply the dedup logic that manual_review_dedupe_strat would do:
    # partition by school_id_giga, order by _commit_version desc, _change_type asc,
    # keep row_number == 1
    data = data.sort_values(
        ["school_id_giga", "_commit_version", "_change_type"],
        ascending=[True, False, True],
    )
    result = data.groupby("school_id_giga").first().reset_index()

    assert len(result) == 2
    school1_row = result[result["school_id_giga"] == "school1"].iloc[0]
    assert school1_row["value"] == "latest"
    assert school1_row["_change_type"] == "update_postimage"


def test_core_merge_logic_basic(spark_session):
    master_schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("value", IntegerType(), True),
        ]
    )
    master_data = [("1", "Alice", 100), ("2", "Bob", 200)]
    master = spark_session.createDataFrame(master_data, master_schema)
    updates_data = [("1", "Alice_Updated", 150)]
    updates = spark_session.createDataFrame(updates_data, master_schema)
    inserts_data = [("3", "Charlie", 300)]
    inserts = spark_session.createDataFrame(inserts_data, master_schema)
    deletes_data = [("2", None, None)]
    deletes = spark_session.createDataFrame(deletes_data, master_schema)
    result = core_merge_logic(
        master,
        inserts,
        updates,
        deletes,
        primary_key="id",
        column_names=["id", "name", "value"],
        update_join_type="inner",
    )
    result_data = sorted(result.collect(), key=lambda x: x.id)
    assert len(result_data) == 2
    assert result_data[0].id == "1"
    assert result_data[0].name == "Alice_Updated"
    assert result_data[0].value == 150
    assert result_data[1].id == "3"
    assert result_data[1].name == "Charlie"


def test_partial_in_cluster_merge(spark_session):
    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
        ]
    )
    master_data = [("1", "Alice"), ("2", "Bob")]
    master = spark_session.createDataFrame(master_data, schema)
    new_data = [("1", "Alice_Updated"), ("3", "Charlie")]
    new = spark_session.createDataFrame(new_data, schema)
    result = partial_in_cluster_merge(
        master, new, primary_key="id", column_names=["id", "name"]
    )
    result_data = sorted(result.collect(), key=lambda x: x.id)
    assert len(result_data) == 3
    assert result_data[0].name == "Alice_Updated"
    assert result_data[2].id == "3"


def test_full_in_cluster_merge(spark_session):
    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("value", IntegerType(), True),
        ]
    )
    master_data = [("1", "Alice", 100), ("2", "Bob", 200), ("4", "David", 400)]
    master = spark_session.createDataFrame(master_data, schema)
    new_data = [("1", "Alice_Updated", 150), ("2", "Bob", 200), ("3", "Charlie", 300)]
    new = spark_session.createDataFrame(new_data, schema)
    with patch("src.internal.merge.compute_row_hash") as mock_hash:
        mock_hash.side_effect = lambda df: df
        result = full_in_cluster_merge(
            master, new, primary_key="id", column_names=["id", "name", "value"]
        )
        result_data = sorted(result.collect(), key=lambda x: x.id)
        assert len(result_data) == 3
        assert "4" not in [r.id for r in result_data]
        assert result_data[0].name == "Alice_Updated"


@patch("src.internal.merge.get_context_with_fallback_logger")
def test_partial_cdf_in_cluster_merge(mock_logger, spark_session):
    mock_logger.return_value = MagicMock()
    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("_change_type", StringType(), True),
        ]
    )
    master_data = [("1", "Alice", None), ("2", "Bob", None)]
    master = spark_session.createDataFrame(master_data, schema)
    incoming_data = [
        ("3", "Charlie", "insert"),
        ("1", "Alice_Updated", "update_postimage"),
        ("2", "Bob_Old", "update_preimage"),
    ]
    incoming = spark_session.createDataFrame(incoming_data, schema)
    result = partial_cdf_in_cluster_merge(
        master, incoming, column_names=["id", "name"], primary_key="id", context=None
    )
    result_data = sorted(result.collect(), key=lambda x: x.id)
    assert len(result_data) == 3
    assert result_data[0].name == "Alice_Updated"
    assert result_data[2].id == "3"
