# import shutil
# import tempfile
# from datetime import datetime
#
# import pytest
# from delta import configure_spark_with_delta_pip
# from pyspark.sql import Row, SparkSession
# from src.assets.upload_processing.parquet_to_delta import (
#     _is_new_or_modified,
#     _read_manifest,
#     _record_manifest_entry,
#     convert_parquets_to_delta,
#     ParquetToDeltaConfig,
# )
# from src.utils.adls import ADLSFileClient
#
#
# @pytest.fixture(scope="function")
# def spark() -> SparkSession:
#     warehouse_dir = tempfile.mkdtemp()
#     builder = (
#         SparkSession.builder.master("local[1]")
#         .appName("test-manifest")
#         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
#         .config(
#             "spark.sql.catalog.spark_catalog",
#             "org.apache.spark.sql.delta.catalog.DeltaCatalog",
#         )
#         .config("spark.sql.warehouse.dir", warehouse_dir)
#     )
#     spark = configure_spark_with_delta_pip(builder).getOrCreate()
#     yield spark
#     spark.stop()
#     shutil.rmtree(warehouse_dir)
#
#
# def test_is_new_or_modified_returns_false_for_existing_file(spark):
#     schema = "file_path STRING, checksum STRING"
#     manifest_df = spark.createDataFrame(
#         [
#             Row(
#                 file_path="abfss://path/file.parquet",
#                 checksum="abc123",
#             )
#         ],
#         schema=schema
#     )
#
#     result = _is_new_or_modified(
#         manifest_df,
#         file_path="abfss://path/file.parquet",
#         checksum="abc123",
#     )
#
#     assert result is False
#
#
# def test_is_new_or_modified_returns_true_for_new_file(spark):
#     schema = "file_path STRING, checksum STRING"
#     manifest_df = spark.createDataFrame(
#         [
#             Row(
#                 file_path="abfss://path/file.parquet",
#                 checksum="abc123",
#             )
#         ],
#         schema=schema
#     )
#
#     result = _is_new_or_modified(
#         manifest_df,
#         file_path="abfss://path/other.parquet",
#         checksum="abc123",
#     )
#
#     assert result is True
#
#
# def test_is_new_or_modified_returns_true_for_modified_file(spark):
#     schema = "file_path STRING, checksum STRING"
#     manifest_df = spark.createDataFrame(
#         [
#             Row(
#                 file_path="abfss://path/file.parquet",
#                 checksum="abc123",
#             )
#         ],
#         schema=schema
#     )
#
#     result = _is_new_or_modified(
#         manifest_df,
#         file_path="abfss://path/file.parquet",
#         checksum="different_checksum",
#     )
#
#     assert result is True
#
#
# def test_record_manifest_entry_writes_row(spark: SparkSession):
#     schema_name = "default"
#     table_name = "_test_manifest"
#
#     spark.sql(f"DROP TABLE IF EXISTS {schema_name}.{table_name}")
#
#     _record_manifest_entry(
#         spark,
#         schema_name,
#         table_name,
#         file_path="abfss://path/file.parquet",
#         file_size=123,
#         last_modified=datetime(2024, 1, 1),
#         checksum="abc123",
#         table_name="test_table",
#     )
#
#     df = spark.read.table(f"{schema_name}.{table_name}")
#
#     assert df.count() == 1
#
#     row = df.collect()[0]
#     assert row.file_path == "abfss://path/file.parquet"
#     assert row.checksum == "abc123"
#     assert row.table_name == "test_table"
#
#
# def test_read_manifest_creates_table_if_missing(spark: SparkSession):
#     schema_name = "default"
#     table_name = "_manifest_create_test"
#
#     spark.sql(f"DROP TABLE IF EXISTS {schema_name}.{table_name}")
#
#     df = _read_manifest(
#         spark,
#         schema_name,
#         table_name,
#     )
#
#     assert df.count() == 0
#     assert table_name in [t.name for t in spark.catalog.listTables(schema_name)]
