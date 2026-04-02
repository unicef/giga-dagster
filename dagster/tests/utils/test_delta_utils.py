from unittest.mock import MagicMock, patch

from src.constants import DataTier
from src.utils.delta import (
    check_table_exists,
    create_schema,
    execute_query_with_error_handler,
    get_change_operation_counts,
)


def test_check_table_exists(spark_session):
    # Mock spark.catalog.tableExists and DeltaTable.isDeltaTable
    with (
        patch.object(spark_session.catalog, "tableExists", return_value=True),
        patch("src.utils.delta.DeltaTable.isDeltaTable", return_value=True),
    ):
        # We need to ensure the internal construct_schema_name_for_tier works or is mocked
        assert (
            check_table_exists(
                spark_session, "school_geolocation", "table", DataTier.SILVER
            )
            is True
        )


def test_create_schema(spark_session):
    with patch.object(spark_session, "sql") as mock_sql:
        create_schema(spark_session, "test_schema")
        mock_sql.assert_called_with("CREATE SCHEMA IF NOT EXISTS `test_schema`")


def test_get_change_operation_counts(spark_session):
    data = [("insert",), ("update_postimage",), ("delete",), ("update_postimage",)]
    df = spark_session.createDataFrame(data, ["_change_type"])
    counts = get_change_operation_counts(df)
    assert counts["added"] == 1
    assert counts["modified"] == 2
    assert counts["deleted"] == 1


def test_execute_query_with_error_handler_success():
    spark = MagicMock()
    query = MagicMock()
    context = MagicMock()
    execute_query_with_error_handler(spark, query, "schema", "table", context)
    query.execute.assert_called_once()
