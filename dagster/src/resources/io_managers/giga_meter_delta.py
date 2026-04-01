from dagster_pyspark import PySparkResource
from pyspark.sql import DataFrame, SparkSession

from dagster import ConfigurableIOManager, InputContext, OutputContext
from src.settings import settings
from src.utils.delta import create_schema


class GigaMeterDeltaIOManager(ConfigurableIOManager):
    """IO Manager that reads Parquet files and writes to Delta Lake tables.

    Used by the Giga Meter pipeline to persist ingested connectivity
    ping data into a Bronze Delta table with append-only semantics
    and automatic schema merging.
    """

    pyspark: PySparkResource

    def _get_spark_session(self) -> SparkSession:
        return self.pyspark.spark_session

    def handle_output(self, context: OutputContext, output: DataFrame | None):
        """Write a Spark DataFrame to a Delta Lake table.

        Creates the table on first write (overwrite mode).
        Subsequent writes append with schema merging enabled.
        """
        if output is None:
            context.log.info("Output is None, skipping Delta write.")
            return

        config = context.step_context.op_config
        schema_name = config["target_schema"]
        table_name = config["target_table"]
        table_full_name = f"{schema_name}.{table_name}"

        spark = self._get_spark_session()

        create_schema(spark, schema_name)

        # Disable vectorized parquet reader for the entire write operation.
        # This prevents INT64 vs IntegerType conversion failures when Spark
        # lazily reads the source parquet during saveAsTable.
        original_vectorized = spark.conf.get(
            "spark.sql.parquet.enableVectorizedReader", "true"
        )
        spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

        try:
            context.log.info(
                f"Appending to table {table_full_name} (creates if not exists)."
            )
            (
                output.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(table_full_name)
            )
        finally:
            spark.conf.set(
                "spark.sql.parquet.enableVectorizedReader", original_vectorized
            )

        context.log.info(f"Successfully wrote to Delta table {table_full_name}.")

    def load_input(self, context: InputContext) -> DataFrame | None:
        """Read a Parquet file from ADLS.

        Returns the DataFrame if successful, or None if the file
        is missing or unreadable.
        """
        config = context.upstream_output.step_context.op_config
        upload_path = config.get("upload_path")

        if not upload_path:
            context.log.warning("No upload path in config, returning None.")
            return None

        spark = self._get_spark_session()
        full_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{upload_path}"

        try:
            df = spark.read.parquet(full_path)
            context.log.info(f"Successfully read Parquet from {full_path}.")
            return df
        except Exception as exc:
            context.log.error(f"Failed to read Parquet from {full_path}: {exc}")
            return None
