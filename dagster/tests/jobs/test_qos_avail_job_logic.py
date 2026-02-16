from unittest.mock import MagicMock, PropertyMock, patch

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
)
from src.jobs.qos_availability import process_availability


def test_process_availability_full_logic(
    spark_session, mock_spark_resource, op_context
):
    context = op_context
    context.log.info = MagicMock()
    context.log.warning = MagicMock()

    events_data = [
        ("d1", "online", "2023-01-01 10:00:00"),
        ("d1", "offline", "2023-01-01 10:45:00"),
        ("d1", "online", "2023-01-01 11:00:00"),
    ]

    events_df = spark_session.createDataFrame(
        events_data, ["device_id", "status", "ts_str"]
    )
    events_df = events_df.withColumn(
        "timestamp", F.col("ts_str").cast("timestamp")
    ).drop("ts_str")

    device_meta_df = spark_session.createDataFrame(
        [("d1", "Room A", "100")], ["serial", "meraki_name_room", "school_id_govt"]
    )

    school_master_df = spark_session.createDataFrame(
        [("100", "GIGA_1")], ["school_id_govt", "school_id_giga"]
    )

    def read_table_side_effect(table_name):
        if table_name == "qos_hourly.vct":
            raise Exception("Table not found")
        elif table_name == "qos_availability.vct":
            return events_df
        elif table_name == "custom_dataset.device_matched":
            return device_meta_df
        elif table_name == "school_master.vct":
            return school_master_df
        else:
            return spark_session.createDataFrame([], StructType([]))

    with (
        patch("pyspark.sql.DataFrameReader.table", side_effect=read_table_side_effect),
        patch("src.jobs.qos_availability.DeltaTable") as MockDeltaTable,
        patch("pyspark.sql.DataFrame.write", new_callable=PropertyMock) as _,
        patch("pyspark.sql.functions.current_timestamp") as mock_curr_ts,
    ):
        mock_curr_ts.return_value = F.lit("2023-01-02 00:00:00").cast("timestamp")

        MockDeltaTable.isDeltaTable.return_value = False

        process_availability(context=context, spark=mock_spark_resource)

        assert context.log.info.call_count >= 1


def test_process_availability_incremental_logic(
    spark_session, mock_spark_resource, op_context
):
    context = op_context

    qos_vct_df = spark_session.createDataFrame(
        [("d1", "2023-01-01 09:00:00")], ["device_id", "ts_str"]
    )
    qos_vct_df = qos_vct_df.withColumn(
        "timestamp", F.col("ts_str").cast("timestamp")
    ).drop("ts_str")

    events_data = [
        ("d1", "online", "2023-01-01 08:00:00"),
        ("d1", "offline", "2023-01-01 10:00:00"),
    ]
    events_df = spark_session.createDataFrame(
        events_data, ["device_id", "status", "ts_str"]
    )
    events_df = events_df.withColumn(
        "timestamp", F.col("ts_str").cast("timestamp")
    ).drop("ts_str")

    device_meta_df = spark_session.createDataFrame(
        [("d1", "Room A", "100")], ["serial", "meraki_name_room", "school_id_govt"]
    )
    school_master_df = spark_session.createDataFrame(
        [("100", "GIGA_1")], ["school_id_govt", "school_id_giga"]
    )

    def read_table_side_effect(table_name):
        if table_name == "qos_hourly.vct":
            return qos_vct_df
        elif table_name == "qos_availability.vct":
            return events_df
        elif table_name == "custom_dataset.device_matched":
            return device_meta_df
        elif table_name == "school_master.vct":
            return school_master_df
        else:
            return spark_session.createDataFrame([], StructType([]))

    with (
        patch("pyspark.sql.DataFrameReader.table", side_effect=read_table_side_effect),
        patch("src.jobs.qos_availability.DeltaTable") as MockDeltaTable,
        patch("pyspark.sql.functions.current_timestamp") as mock_curr_ts,
    ):
        mock_curr_ts.return_value = F.lit("2023-01-02 00:00:00").cast("timestamp")

        MockDeltaTable.isDeltaTable.return_value = True
        mock_delta_table = MagicMock()
        MockDeltaTable.forName.return_value = mock_delta_table

        process_availability(context=context, spark=mock_spark_resource)

        mock_delta_table.alias.return_value.merge.assert_called()
