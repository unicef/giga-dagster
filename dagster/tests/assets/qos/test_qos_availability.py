from unittest.mock import MagicMock, patch

from pyspark.sql.types import StringType, StructField, StructType
from src.assets.qos.qos_availability import (
    publish_qos_availability_to_gold,
    qos_availability_raw,
    qos_availability_transforms,
)


def test_qos_availability_raw(mock_adls_client, mock_file_config, op_context):
    context = op_context
    mock_adls_client.download_raw.return_value = b"col1,col2\n1,2"
    result = qos_availability_raw(context, mock_adls_client, mock_file_config)
    assert result.value == b"col1,col2\n1,2"
    mock_adls_client.download_raw.assert_called_with(mock_file_config.filepath)


def test_qos_availability_transforms(spark_session, mock_file_config, op_context):
    context = op_context
    raw_bytes = b"school_id_giga,timestamp,device_id\n1,2023-01-01,d1"
    mock_spark = MagicMock()
    mock_spark.spark_session = spark_session
    result = qos_availability_transforms(
        context, mock_spark, mock_file_config, raw_bytes
    )
    df = result.value
    assert "signature" in df.columns
    assert "gigasync_id" in df.columns
    assert "date" in df.columns
    assert len(df) == 1


@patch("src.assets.qos.qos_availability.transform_types")
def test_publish_qos_availability_to_gold(
    mock_transform, spark_session, mock_file_config, op_context
):
    context = op_context
    schema = StructType(
        [
            StructField("col1", StringType(), True),
            StructField("void_col", StringType(), True),
        ]
    )
    data = [("a", None)]
    df = spark_session.createDataFrame(data, schema)
    mock_transform.return_value = df
    mock_spark = MagicMock()
    result = publish_qos_availability_to_gold(context, mock_spark, mock_file_config, df)
    mock_transform.assert_called()
    assert result.value is not None
