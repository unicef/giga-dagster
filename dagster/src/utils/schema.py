from delta import DeltaTable
from models import Schema
from pyspark import sql
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructField

from dagster import (
    AssetExecutionContext,
    InputContext,
    OpExecutionContext,
    OutputContext,
)
from src.constants import DataTier, constants
from src.spark.config_expectations import config


def _get_type_mapping(data_type: str):
    """Map a data type string to its corresponding TypeMapping from constants.

    Handles case-insensitivity and common aliases (e.g., 'INT' -> 'integer').
    """
    normalized_type = data_type.lower()
    # Handle common aliases from config_expectations
    if normalized_type == "int":
        normalized_type = "integer"

    return getattr(constants.TYPE_MAPPINGS, normalized_type)


def get_schema_name(
    context: InputContext | OutputContext | OpExecutionContext | AssetExecutionContext,
) -> str:
    if isinstance(context, InputContext | OutputContext):
        return context.step_context.op_config["metastore_schema"]
    return context.op_config["metastore_schema"]


def _get_fallback_schema_df(spark: SparkSession, schema_name: str) -> sql.DataFrame:
    """Return a fallback schema DataFrame from hardcoded configs if Delta table is missing."""
    if schema_name == "school_geolocation":
        columns = config.COLUMNS_EXCEPT_SCHOOL_ID_GEOLOCATION + [
            "school_id_govt",
            "school_id_giga",
        ]
        data_types = dict(config.DATA_TYPES)

        fallback_data = []
        for col_name in columns:
            data_type = data_types.get(col_name, "string")
            fallback_data.append(
                {
                    "id": col_name,
                    "name": col_name,
                    "data_type": data_type,
                    "is_nullable": True,
                    "is_important": False,
                    "is_system_generated": False,
                    "description": "",
                    "primary_key": col_name in config.UNIQUE_COLUMNS_GEOLOCATION,
                    "partition_order": None,
                    "license": None,
                    "units": None,
                    "hint": None,
                }
            )

        # Define the schema explicitly to match SchemaModel
        schema = Schema.schema
        return spark.createDataFrame(fallback_data, schema=schema)

    raise ValueError(f"No fallback schema available for `{schema_name}`")


def get_schema_table(spark: SparkSession, schema_name: str) -> sql.DataFrame:
    metaschema_name = Schema.__schema_name__
    full_table_name = f"{metaschema_name}.{schema_name}"

    try:
        # This should be cheap if the migrations.migrate_schema asset is caching the table properly
        return DeltaTable.forName(spark, full_table_name).toDF()
    except AnalysisException as e:
        if "DELTA_TABLE_NOT_FOUND" in str(e):
            return _get_fallback_schema_df(spark, schema_name)
        raise e


def get_schema_columns(spark: SparkSession, schema_name: str) -> list[StructField]:
    df = get_schema_table(spark, schema_name)
    return [
        StructField(
            row.name,
            _get_type_mapping(row.data_type).pyspark(),
            row.is_nullable,
        )
        for row in df.collect()
    ]


def get_schema_columns_with_id(
    spark: SparkSession, schema_name: str
) -> list[tuple[str, StructField]]:
    """Return schema columns paired with their stable UUID id.

    Each tuple is ``(id, StructField)``.  The ``id`` is the fixed UUID
    assigned to the column in the schema CSV stored in ADLS.  By comparing
    IDs between the reference schema and an existing Delta table we can
    detect:

    * **Renames** – same ID, different ``StructField.name``
    * **Deletes** – ID present in the table but absent from the reference
    * **Adds**    – ID present in the reference but absent from the table
    """
    df = get_schema_table(spark, schema_name)
    return [
        (
            row.id,
            StructField(
                row.name,
                _get_type_mapping(row.data_type).pyspark(),
                row.is_nullable,
            ),
        )
        for row in df.collect()
    ]


def get_schema_column_descriptions(
    spark: SparkSession, schema_name: str
) -> dict[str:str]:
    df = get_schema_table(spark, schema_name)
    return {row.name: row.description for row in df.collect()}


def get_schema_columns_datahub(spark: SparkSession, schema_name: str) -> list[tuple]:
    df = get_schema_table(spark, schema_name)
    return [
        (row.name, _get_type_mapping(row.data_type).datahub()) for row in df.collect()
    ]


def get_primary_key(spark: SparkSession, schema_name: str) -> str:
    df = get_schema_table(spark, schema_name)
    return df.filter(df["primary_key"]).first().name


def get_partition_columns(spark: SparkSession, schema_name: str) -> list[str]:
    df = get_schema_table(spark, schema_name)
    return [
        row.name
        for row in df.filter(
            col("partition_order").isNotNull() & (col("partition_order") > 0),
        )
        .orderBy("partition_order")
        .select("name")
        .collect()
    ]


def construct_schema_name_for_tier(schema_name: str, tier: DataTier = None) -> str:
    if tier is not None and tier in [
        DataTier.SILVER,
        DataTier.STAGING,
        DataTier.MANUAL_REJECTED,
    ]:
        return f"{schema_name.lower()}_{tier.value}"
    return schema_name.lower()


def construct_full_table_name(schema_name: str, table_name: str) -> str:
    return f"{schema_name.lower()}.{table_name.lower()}"
