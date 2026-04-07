from unittest.mock import patch

from pyspark.sql.types import FloatType, StringType, StructField, StructType
from src.constants import UploadMode
from src.spark.transform_functions import (
    add_missing_columns,
    add_missing_values,
    bronze_prereq_columns,
    clean_type_connectivity,
    column_mapping_rename,
    create_education_level,
    create_health_id_giga,
    create_uzbekistan_school_name,
    generate_uuid,
    get_connectivity_type_root,
    standardize_connectivity_type,
    standardize_internet_speed,
)


def test_standardize_connectivity_type(spark_session):
    data = [("Fiber Optic",)]
    schema = StructType([StructField("connectivity_type_govt", StringType(), True)])
    df = spark_session.createDataFrame(data, schema)
    # Test CREATE mode
    result_df = standardize_connectivity_type(
        df, UploadMode.CREATE.value, ["connectivity_type_govt"]
    )
    row = result_df.collect()[0]
    assert row["connectivity_type"] == "fibre"
    assert row["connectivity_type_root"] == "wired"

    # Test UPDATE mode with missing column
    df_update = spark_session.createDataFrame([("Fiber Optic",)], schema)
    result_df_update = standardize_connectivity_type(
        df_update, UploadMode.UPDATE.value, []
    )
    assert "connectivity_type" not in result_df_update.columns


def test_bronze_prereq_columns(spark_session):
    data = [("val1", "val2")]
    schema = StructType(
        [
            StructField("col1", StringType(), True),
            StructField("col2", StringType(), True),
        ]
    )
    df = spark_session.createDataFrame(data, schema)
    schema_cols = [StructField("col1", StringType(), True)]
    result_df = bronze_prereq_columns(df, schema_cols)
    assert result_df.columns == ["col1"]


def test_add_missing_values(spark_session):
    data = [(None,)]
    schema = StructType([StructField("col1", StringType(), True)])
    df = spark_session.createDataFrame(data, schema)
    schema_cols = [StructField("col1", StringType(), True)]
    result_df = add_missing_values(df, schema_cols)
    assert result_df.collect()[0]["col1"] == "Unknown"


@patch("src.spark.transform_functions.get_nocodb_table_id_from_name")
@patch("src.spark.transform_functions.get_nocodb_table_as_key_value_mapping")
def test_create_education_level_update(mock_get_mapping, mock_get_id, spark_session):
    mock_get_id.return_value = "table_id"
    mock_get_mapping.return_value = {"Primary": "Primary (Standard)"}
    data = [("Primary", "Something")]
    schema = StructType(
        [
            StructField("education_level_govt", StringType(), True),
            StructField("education_level", StringType(), True),
        ]
    )
    df = spark_session.createDataFrame(data, schema)
    # Mode UPDATE, education_level_govt in uploaded_columns
    result_df = create_education_level(df, UploadMode.UPDATE.value, ["education_level"])
    row = result_df.collect()[0]
    # Coalesce should take existing education_level if not null
    assert row["education_level"] == "Something"


def test_generate_uuid():
    input_str = "test_string"
    uuid1 = generate_uuid(input_str)
    uuid2 = generate_uuid(input_str)
    assert uuid1 == uuid2
    assert isinstance(uuid1, str)
    assert len(uuid1) > 0


@patch("src.spark.transform_functions.get_nocodb_table_id_from_name")
@patch("src.spark.transform_functions.get_nocodb_table_as_key_value_mapping")
def test_create_education_level(mock_get_mapping, mock_get_id, spark_session):
    mock_get_id.return_value = "table_id"
    mock_get_mapping.return_value = {
        "Primary": "Primary (Standard)",
        "Secondary": "Secondary (Standard)",
    }
    data = [("Primary", None), ("Secondary", None), ("Unknown", None)]
    schema = StructType(
        [
            StructField("education_level_govt", StringType(), True),
            StructField("education_level", StringType(), True),
        ]
    )
    df = spark_session.createDataFrame(data, schema)
    uploaded_columns = ["education_level_govt"]
    result_df = create_education_level(df, UploadMode.CREATE.value, uploaded_columns)
    results = {
        row["education_level_govt"]: row["education_level"]
        for row in result_df.collect()
    }
    assert results["Primary"] == "Primary (Standard)"
    assert results["Secondary"] == "Secondary (Standard)"
    assert results["Unknown"] == "Unknown"


def test_create_health_id_giga(spark_session):
    data = [("Facility A", 10.0, 20.0, None)]
    schema = StructType(
        [
            StructField("facility_name", StringType(), True),
            StructField("latitude", FloatType(), True),
            StructField("longitude", FloatType(), True),
            StructField("health_id_giga", StringType(), True),
        ]
    )
    df = spark_session.createDataFrame(data, schema)
    result_df = create_health_id_giga(df)
    row = result_df.collect()[0]
    assert row["health_id_giga"] is not None
    assert len(row["health_id_giga"]) == 36  # UUID length


def test_standardize_internet_speed(spark_session):
    data = [("10 Mbps"), ("20.5 MB/s"), ("No speed")]
    schema = StructType([StructField("download_speed_govt", StringType(), True)])
    df = spark_session.createDataFrame([(d,) for d in data], schema)
    result_df = standardize_internet_speed(df)
    results = [row["download_speed_govt"] for row in result_df.collect()]
    assert results[0] == 10.0
    assert results[1] == 20.5
    assert results[2] is None


def test_clean_type_connectivity():
    assert clean_type_connectivity("Fiber Optic") == "fibre"
    assert clean_type_connectivity("4G LTE") == "cellular"
    assert clean_type_connectivity("Satellite") == "satellite"
    assert clean_type_connectivity("Unknown") == "unknown"
    assert clean_type_connectivity(None) == "unknown"


def test_get_connectivity_type_root():
    assert get_connectivity_type_root("fibre") == "wired"
    assert get_connectivity_type_root("cellular") == "wireless"
    assert get_connectivity_type_root("unknown") == "unknown_connectivity_type"


def test_create_uzbekistan_school_name(spark_session):
    data = [("School A", "Dist 1", "City 1", "Reg 1")]
    schema = StructType(
        [
            StructField("school_name", StringType(), True),
            StructField("district", StringType(), True),
            StructField("city", StringType(), True),
            StructField("region", StringType(), True),
        ]
    )
    df = spark_session.createDataFrame(data, schema)
    result_df = create_uzbekistan_school_name(df)
    row = result_df.collect()[0]
    assert row["school_name"] == "School A,Dist 1,Reg 1"


def test_column_mapping_rename(spark_session):
    data = [("val1", "val2")]
    schema = StructType(
        [
            StructField("old_col1", StringType(), True),
            StructField("old_col2", StringType(), True),
        ]
    )
    df = spark_session.createDataFrame(data, schema)
    mapping = {"old_col1": "new_col1", "old_col2": "new_col2"}
    result_df, filtered_mapping = column_mapping_rename(df, mapping)
    assert "new_col1" in result_df.columns
    assert "new_col2" in result_df.columns
    assert filtered_mapping == mapping


def test_add_missing_columns(spark_session):
    data = [("val1",)]
    schema = StructType([StructField("col1", StringType(), True)])
    df = spark_session.createDataFrame(data, schema)
    schema_cols = [
        StructField("col1", StringType(), True),
        StructField("col2", StringType(), True),
    ]
    result_df = add_missing_columns(df, schema_cols)
    assert "col2" in result_df.columns
    assert result_df.collect()[0]["col2"] is None


from src.spark.transform_functions import create_bronze_layer_columns


@patch("src.spark.transform_functions.create_education_level")
@patch("src.spark.transform_functions.create_school_id_giga")
@patch("src.spark.transform_functions.add_admin_columns")
@patch("src.spark.transform_functions.add_disputed_region_column")
def test_create_bronze_layer_columns(
    mock_disputed, mock_admin, mock_id, mock_edu, spark_session
):
    def mock_admin_fn(df, country_code_iso3, admin_level):
        from pyspark.sql import functions as f

        return df.withColumn(admin_level, f.lit("mock_val")).withColumn(
            f"{admin_level}_id_giga", f.lit("mock_id")
        )

    mock_edu.side_effect = lambda df, *args, **kwargs: df
    mock_id.side_effect = lambda df, *args, **kwargs: df
    mock_admin.side_effect = mock_admin_fn
    mock_disputed.side_effect = lambda df, *args, **kwargs: df

    data_df = [("1", "A", None)]
    schema_df = StructType(
        [
            StructField("school_id_govt", StringType(), True),
            StructField("col_df", StringType(), True),
            StructField("school_id_govt_type", StringType(), True),
        ]
    )
    df = spark_session.createDataFrame(data_df, schema_df)

    data_silver = [("1", "S", None)]
    schema_silver = StructType(
        [
            StructField("school_id_govt", StringType(), True),
            StructField("col_silver", StringType(), True),
            StructField("school_id_govt_type", StringType(), True),
        ]
    )
    silver = spark_session.createDataFrame(data_silver, schema_silver)

    result_df = create_bronze_layer_columns(
        df, silver, "BRA", UploadMode.CREATE.value, ["school_id_govt"]
    )

    assert "col_silver" in result_df.columns
    assert result_df.collect()[0]["col_silver"] == "S"
    assert result_df.collect()[0]["school_id_govt_type"] == "Unknown"

    # Test with latitude/longitude to trigger admin columns
    # Re-mock to reset call count
    mock_admin.reset_mock()
    data_df_geo = [("1", "A", None, 1.2, 3.4)]
    schema_df_geo = StructType(
        [
            StructField("school_id_govt", StringType(), True),
            StructField("col_df", StringType(), True),
            StructField("school_id_govt_type", StringType(), True),
            StructField("latitude", FloatType(), True),
            StructField("longitude", FloatType(), True),
        ]
    )
    df_geo = spark_session.createDataFrame(data_df_geo, schema_df_geo)

    data_silver_geo = [("1", "S", None, 1.2, 3.4)]
    schema_silver_geo = StructType(
        [
            StructField("school_id_govt", StringType(), True),
            StructField("col_silver", StringType(), True),
            StructField("school_id_govt_type", StringType(), True),
            StructField("latitude", FloatType(), True),
            StructField("longitude", FloatType(), True),
        ]
    )
    silver_geo = spark_session.createDataFrame(data_silver_geo, schema_silver_geo)

    create_bronze_layer_columns(
        df_geo,
        silver_geo,
        "BRA",
        UploadMode.CREATE.value,
        ["school_id_govt", "latitude", "longitude"],
    )
    assert mock_admin.call_count > 0
