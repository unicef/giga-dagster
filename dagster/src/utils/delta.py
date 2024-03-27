from delta.tables import DeltaTableBuilder
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import SparkSession

from dagster import InputContext, OpExecutionContext, OutputContext


def execute_query_with_error_handler(
    context: InputContext | OutputContext | OpExecutionContext,
    spark: SparkSession,
    query: DeltaTableBuilder,
    schema_name: str,
    table_name: str,
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
