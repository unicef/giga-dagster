import os

from dagster_pyspark import PySparkResource
from models import Schema
from pyspark.sql import SparkSession
from src.utils.adls import ADLSFileClient
from src.utils.delta import _ensure_column_mapping_enabled
from src.utils.sentry import capture_op_exceptions

from dagster import OpExecutionContext, asset

from .core import (
    get_filepath,
    save_schema_delta_table,
    validate_raw_schema,
)


@asset
@capture_op_exceptions
def add_default_value_to_metaschemas(
    context: OpExecutionContext, spark: PySparkResource
) -> None:
    s: SparkSession = spark.spark_session
    schema_name = Schema.__schema_name__
    tables = [
        "school_geolocation",
        "school_master",
        "school_coverage",
        "school_reference",
    ]
    for table in tables:
        full = f"`{schema_name}`.`{table}`"
        existing = {r.col_name for r in s.sql(f"SHOW COLUMNS IN {full}").collect()}
        if "default_value" in existing:
            context.log.info(f"{full}: default_value already exists — skipping")
        else:
            s.sql(f"ALTER TABLE {full} ADD COLUMNS (`default_value` STRING)")
            context.log.info(f"{full}: added default_value column")


@asset
@capture_op_exceptions
def initialize_metaschema(_: OpExecutionContext, spark: PySparkResource) -> None:
    s: SparkSession = spark.spark_session
    schema_name = Schema.__schema_name__
    s.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema_name}`")


@asset(deps=["initialize_metaschema"])
@capture_op_exceptions
def migrate_schema(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
) -> None:
    s: SparkSession = spark.spark_session
    filepath = get_filepath(context)
    pdf = adls_file_client.download_csv_as_pandas_dataframe(filepath)

    filename = os.path.splitext(filepath.split("/")[-1])[0]
    schema_name = Schema.__schema_name__
    table_name = filename.replace("-", "_").lower()
    full_table_name = f"{schema_name}.{table_name}"
    df = s.createDataFrame(pdf, schema=Schema.schema)
    df = validate_raw_schema(context, df)
    save_schema_delta_table(context, df)

    if s.catalog.isCached(full_table_name):
        s.catalog.refreshTable(full_table_name)
    else:
        s.catalog.cacheTable(full_table_name)


_DDL_SCHEMA = "school_master"
_DDL_TABLES = ["afg", "bih", "lka"]
_RENAME_OLD = "school_name"
_RENAME_NEW = "school_name_govt"
_DROP_COLUMN = "disputed_region"


@asset
@capture_op_exceptions
def migrate__set_table_properties(
    context: OpExecutionContext,
    spark: PySparkResource,
) -> None:
    s: SparkSession = spark.spark_session
    for tbl in _DDL_TABLES:
        full = f"`{_DDL_SCHEMA}`.`{tbl}`"
        s.sql(
            f"ALTER TABLE {full} SET TBLPROPERTIES ("
            f"'delta.minReaderVersion' = '2', "
            f"'delta.minWriterVersion' = '5', "
            f"'delta.columnMapping.mode' = 'name')"
        )
        context.log.info(f"Set column mapping properties on {full}")


@asset
@capture_op_exceptions
def migrate__rename_column(
    context: OpExecutionContext,
    spark: PySparkResource,
) -> None:
    s: SparkSession = spark.spark_session
    for tbl in _DDL_TABLES:
        full = f"`{_DDL_SCHEMA}`.`{tbl}`"
        _ensure_column_mapping_enabled(s, full, context)
        s.sql(f"ALTER TABLE {full} RENAME COLUMN `{_RENAME_OLD}` TO `{_RENAME_NEW}`")
        context.log.info(f"Renamed '{_RENAME_OLD}' → '{_RENAME_NEW}' on {full}")


@asset
@capture_op_exceptions
def migrate__drop_column(
    context: OpExecutionContext,
    spark: PySparkResource,
) -> None:
    s: SparkSession = spark.spark_session
    for tbl in _DDL_TABLES:
        full = f"`{_DDL_SCHEMA}`.`{tbl}`"
        _ensure_column_mapping_enabled(s, full, context)
        s.sql(f"ALTER TABLE {full} DROP COLUMN `{_DROP_COLUMN}`")
        context.log.info(f"Dropped '{_DROP_COLUMN}' from {full}")
