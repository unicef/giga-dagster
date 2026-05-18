import os

from dagster_pyspark import PySparkResource
from models import Schema
from pyspark.sql import SparkSession
from src.utils.adls import ADLSFileClient
from src.utils.delta import _ensure_column_mapping_enabled
from src.utils.sentry import capture_op_exceptions

from dagster import Config, MetadataValue, OpExecutionContext, Output, asset

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


class SetTablePropertiesConfig(Config):
    schema_name: str
    table_name: str


@asset
@capture_op_exceptions
def migrate__set_table_properties(
    context: OpExecutionContext,
    config: SetTablePropertiesConfig,
    spark: PySparkResource,
) -> Output:
    s: SparkSession = spark.spark_session
    full = f"`{config.schema_name}`.`{config.table_name}`"
    s.sql(
        f"ALTER TABLE {full} SET TBLPROPERTIES ("
        f"'delta.minReaderVersion' = '2', "
        f"'delta.minWriterVersion' = '5', "
        f"'delta.columnMapping.mode' = 'name')"
    )
    context.log.info(f"Set column mapping properties on {full}")
    props = {r.key: r.value for r in s.sql(f"SHOW TBLPROPERTIES {full}").collect()}
    return Output(
        None,
        metadata={"tblproperties": MetadataValue.json(props)},
    )


class RenameColumnConfig(Config):
    schema_name: str
    table_name: str
    old_column_name: str
    new_column_name: str


@asset
@capture_op_exceptions
def migrate__rename_column(
    context: OpExecutionContext,
    config: RenameColumnConfig,
    spark: PySparkResource,
) -> None:
    s: SparkSession = spark.spark_session
    full = f"`{config.schema_name}`.`{config.table_name}`"
    _ensure_column_mapping_enabled(s, full, context)
    s.sql(
        f"ALTER TABLE {full} RENAME COLUMN `{config.old_column_name}` TO `{config.new_column_name}`"
    )
    context.log.info(
        f"Renamed column '{config.old_column_name}' → '{config.new_column_name}' on {full}"
    )


class DropColumnConfig(Config):
    schema_name: str
    table_name: str
    column_name: str


@asset
@capture_op_exceptions
def migrate__drop_column(
    context: OpExecutionContext,
    config: DropColumnConfig,
    spark: PySparkResource,
) -> None:
    s: SparkSession = spark.spark_session
    full = f"`{config.schema_name}`.`{config.table_name}`"
    _ensure_column_mapping_enabled(s, full, context)
    s.sql(f"ALTER TABLE {full} DROP COLUMN `{config.column_name}`")
    context.log.info(f"Dropped column '{config.column_name}' from {full}")
