from delta.tables import DeltaTable, DeltaTableBuilder
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType

from dagster import InputContext, OpExecutionContext, OutputContext


def execute_query_with_error_handler(
    spark: SparkSession,
    query: DeltaTableBuilder,
    schema_name: str,
    table_name: str,
    context: InputContext | OutputContext | OpExecutionContext,
):
    full_table_name = f"{schema_name}.{table_name}"

    try:
        query.execute()
    except AnalysisException as exc:
        if "DELTA_TABLE_NOT_FOUND" in str(exc):
            # This error gets raised when you delete the Delta Table in ADLS and subsequently try to re-ingest the
            # same table. Its corresponding entry in the metastore needs to be dropped first.
            #
            # Deleting a table in ADLS does not drop its metastore entry; the inverse is also true.
            context.log.warning(
                f"Attempting to drop metastore entry for `{full_table_name}`..."
            )
            spark.sql(f"DROP TABLE `{schema_name}`.`{table_name.lower()}`")
            query.execute()
            context.log.info("ok")
        else:
            raise exc


def create_delta_table(
    spark: SparkSession,
    schema_name: str,
    table_name: str,
    columns: StructType | list[StructField],
    context: InputContext | OutputContext | OpExecutionContext,
    if_not_exists=False,
    replace=False,
):
    if if_not_exists and replace:
        raise Exception("Only one of `if_not_exists` or `replace` can be set to True.")

    create_stmt = DeltaTable.create

    if if_not_exists:
        create_stmt = DeltaTable.createIfNotExists
    if replace:
        create_stmt = DeltaTable.createOrReplace

    query = (
        create_stmt(spark)
        .tableName(table_name)
        .addColumns(columns)
        .property("delta.enableChangeDataFeed", "true")
    )
    execute_query_with_error_handler(spark, query, schema_name, table_name, context)


def create_schema(spark: SparkSession, schema_name: str):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema_name}`")
