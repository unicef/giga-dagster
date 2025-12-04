from dagster_pyspark import PySparkResource
from delta import DeltaTable
from icecream import ic
from pydantic import AnyUrl
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from dagster import InputContext, OutputContext
from src.constants import constants
from src.resources.io_managers.base import BaseConfigurableIOManager
from src.settings import settings
from src.spark.transform_functions import add_missing_columns
from src.utils.adls import ADLSFileClient
from src.utils.delta import build_deduped_merge_query, execute_query_with_error_handler
from src.utils.op_config import FileConfig
from src.utils.schema import (
    construct_full_table_name,
    construct_schema_name_for_tier,
    get_partition_columns,
    get_primary_key,
    get_schema_columns,
)

adls_client = ADLSFileClient()


class ADLSDeltaIOManager(BaseConfigurableIOManager):
    pyspark: PySparkResource

    def handle_output(self, context: OutputContext, output: sql.DataFrame | None):
        context.log.info("Handling DeltaIOManager output...")
        if output is None:
            context.log.info("Output is None, skipping execution.")
            return

        config = FileConfig(**context.step_context.op_config)
        try:
            context.log.info(config.country_code)
            context.log.info(config.domain)
            context.log.info(config.metastore_schema)
            context.log.info(config.tier)
            context.log.info(config.table_name)
            context.log.info(config.destination_filepath)
        except Exception as e:
            context.log.error(f"Error in config: {e}")
            pass
        table_name, table_root_path, table_path = self._get_table_path(context)
        schema_tier_name = construct_schema_name_for_tier(
            config.metastore_schema, config.tier
        )
        full_table_name = f"{schema_tier_name}.{table_name}"
        context.log.info(f"Full table name: {full_table_name}")

        if self._handle_truncate(context, output, config, full_table_name):
            return

        self._create_schema_if_not_exists(schema_tier_name)
        self._create_table_if_not_exists(context, output, schema_tier_name, table_name)
        if config.metastore_schema == "custom_dataset":
            self._overwrite_data(
                output, config.metastore_schema, full_table_name, context
            )
        else:
            self._upsert_data(output, config.metastore_schema, full_table_name, context)

        context.log.info(
            f"Uploaded {table_name} to {settings.SPARK_WAREHOUSE_DIR}/{schema_tier_name}.db in ADLS.",
        )

    def load_input(self, context: InputContext) -> sql.DataFrame:
        context.log.info("Handling DeltaIOManager input...")

        table_name, _, _ = self._get_table_path(context)
        spark = self._get_spark_session()
        config = FileConfig(
            **context.upstream_output.step_context.op_config
        )  # this is a scam -- gives the current asset step's config, not the upstream asset
        schema_name = config.metastore_schema

        schema_tier_name = construct_schema_name_for_tier(schema_name, config.tier)
        context.log.info(f"Schema_tier_name: {schema_tier_name}")

        full_table_name = construct_full_table_name(schema_tier_name, table_name)
        context.log.info(f"full_table_name: {full_table_name}")
        context.log.info(full_table_name)

        dt = DeltaTable.forName(spark, full_table_name)

        context.log.info(
            f"Downloaded {table_name} from {settings.SPARK_WAREHOUSE_DIR}/{schema_name}.db in ADLS.",
        )

        return dt.toDF()

    @staticmethod
    def _get_table_path(
        context: InputContext | OutputContext,
    ) -> tuple[str, str, AnyUrl]:
        config = FileConfig(**context.step_context.op_config)
        table_name = config.table_name if config.table_name else config.country_code
        table_root_path = f"{settings.SPARK_WAREHOUSE_DIR}/{config.metastore_schema}.db"
        return (
            ic(table_name),
            ic(table_root_path),
            ic(f"{settings.AZURE_BLOB_CONNECTION_URI}/{table_root_path}/{table_name}"),
        )

    def _get_spark_session(self) -> SparkSession:
        spark: PySparkResource = self.pyspark
        s: SparkSession = spark.spark_session
        return s

    def _create_schema_if_not_exists(self, schema_name: str):
        spark = self._get_spark_session()
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema_name}`")

    def _create_table_if_not_exists(
        self,
        context: InputContext | OutputContext,
        data: sql.DataFrame,
        schema_name_for_tier: str,
        table_name: str,
    ):
        spark = self._get_spark_session()
        full_table_name = construct_full_table_name(schema_name_for_tier, table_name)
        columns_schema_name = FileConfig(
            **context.step_context.op_config
        ).metastore_schema

        if schema_name_for_tier in ["qos", "qos_raw", "qos_availability"]:
            columns = data.schema.fields
            partition_columns = ["date"]
        elif schema_name_for_tier == "custom_dataset":
            columns = data.schema.fields
            partition_columns = []
        else:
            columns = get_schema_columns(spark, columns_schema_name)
            partition_columns = get_partition_columns(spark, columns_schema_name)

        query = (
            DeltaTable.createIfNotExists(spark)
            .tableName(full_table_name)
            .addColumns(columns)
        )

        if len(partition_columns) > 0:
            query.partitionedBy(*partition_columns)

        query = query.property("delta.enableChangeDataFeed", "true")
        if schema_name_for_tier in ["qos", "qos_raw", "qos_availability"]:
            query = query.property(
                "delta.logRetentionDuration", constants.qos_retention_period
            )
        else:
            query = query.property(
                "delta.logRetentionDuration", constants.school_master_retention_period
            )

        execute_query_with_error_handler(
            spark, query, schema_name_for_tier, table_name, context
        )

    def _upsert_data(
        self,
        data: sql.DataFrame,
        schema_name: str,
        full_table_name: str,
        context: OutputContext = None,
    ):
        spark = self._get_spark_session()
        is_qos = schema_name in ["qos", "qos_raw", "qos_availability"]

        if is_qos:
            gold_schema = DeltaTable.forName(spark, full_table_name).toDF().schema
            incoming_schema = data.schema
            gold_columns = {col.name for col in gold_schema.fields}
            gold_columns.discard("signature")
            incoming_columns = {col.name for col in incoming_schema.fields}

            if len(incoming_columns.difference(gold_columns)) > 0:
                # Incoming schema has columns that are not in the gold schema
                dummy_df = spark.createDataFrame([], schema=incoming_schema)
                dummy_df.write.format("delta").option("mergeSchema", "true").mode(
                    "append"
                ).saveAsTable(full_table_name)
            if len(gold_columns.difference(incoming_columns)) > 0:
                # Master schema has columns that are not in the incoming schema
                data = add_missing_columns(data, gold_schema.fields)

            columns = incoming_schema.fields
            primary_key = "gigasync_id"
        else:
            columns = get_schema_columns(spark, schema_name)
            primary_key = get_primary_key(spark, schema_name)

            updated_schema = StructType(columns)
            updated_columns = sorted(updated_schema.fieldNames())

            existing_df = DeltaTable.forName(spark, full_table_name).toDF()
            existing_columns = sorted(existing_df.schema.fieldNames())

            context.log.info(f"incoming schema {data.schema}")
            context.log.info(f"existing schema {existing_df.schema}")

            if updated_columns != existing_columns:
                context.log.info("Updating schema...")

                empty_data = spark.sparkContext.emptyRDD()
                updated_schema_df = spark.createDataFrame(
                    data=empty_data, schema=updated_schema
                )

                (
                    updated_schema_df.write.option("mergeSchema", "true")
                    .format("delta")
                    .mode("append")
                    .saveAsTable(full_table_name)
                )

        update_columns = [c.name for c in columns if c.name != primary_key]
        master = DeltaTable.forName(spark, full_table_name)
        query = build_deduped_merge_query(
            master,
            data,
            primary_key,
            update_columns,
            context=context,
            is_partial_dataset=is_qos,
            is_qos=is_qos,
        )

        if query is not None:
            query.execute()

    def _overwrite_data(
        self,
        data: sql.dataframe,
        schema_name: str,
        full_table_name: str,
        context: OutputContext = None,
    ):
        # spark = self._get_spark_session()
        columns = data.schema.fields
        partition_columns = []

        context.log.info(f"schema: {schema_name}, full table name: {full_table_name}")
        column_names = [col.name for col in columns]
        context.log.info(f"columns: {column_names}")

        # Adaptive repartitioning based on current partition count (avoids expensive count())
        # For small datasets, fewer partitions = faster writes to ADLS
        # For large datasets, more partitions = better parallelism
        current_partitions = data.rdd.getNumPartitions()
        default_parallelism = data.sparkSession.sparkContext.defaultParallelism

        # Use current partition count as a proxy for data size
        # If data already has few partitions, it's likely small
        if current_partitions <= default_parallelism:
            # Small dataset: use moderate partitioning
            num_partitions = min(current_partitions * 2, default_parallelism * 2)
            context.log.info(
                f"Small dataset ({current_partitions} current partitions): "
                f"using {num_partitions} partitions for write"
            )
        else:
            # Large dataset: keep existing partitions (already well-distributed)
            optimal_partitions = default_parallelism * 3
            if current_partitions < optimal_partitions:
                num_partitions = optimal_partitions
                context.log.info(
                    f"Large dataset ({current_partitions} current partitions): "
                    f"increasing to {num_partitions} for better parallelism"
                )
            else:
                # Already well-partitioned, keep as-is to avoid shuffle
                num_partitions = current_partitions
                context.log.info(
                    f"Large dataset ({current_partitions} current partitions): "
                    f"keeping existing partitions (already well-distributed)"
                )

        # Only repartition if it would actually change the partition count
        if num_partitions != current_partitions:
            data = data.repartition(num_partitions)
            context.log.info(
                f"Repartitioned from {current_partitions} to {num_partitions} partitions"
            )
        else:
            context.log.info(
                f"Keeping existing {current_partitions} partitions (already optimal)"
            )

        # overwrite the table with the new data
        context.log.info(f"the table {full_table_name} overwriting with new data")

        # Adaptive maxRecordsPerFile: only limit for large datasets
        write_options = {
            "overwriteSchema": "true",
            "optimizeWrite": "true",
            "autoOptimize.optimizeWrite": "true",
        }

        estimated_rows = num_partitions * 5000

        if estimated_rows > 100000:
            # Large dataset: limit records per file to prevent huge files
            write_options["maxRecordsPerFile"] = "100000"
            context.log.info(
                f"Large dataset (~{estimated_rows} rows): "
                f"limiting to 100K records per file"
            )
        else:
            # Small dataset: let Spark decide file sizes naturally
            context.log.info(
                f"Small dataset (~{estimated_rows} rows): "
                f"no maxRecordsPerFile limit"
            )

        # Log write configuration details
        context.log.info(f"Starting Delta write with options: {write_options}")
        context.log.info(f"Writing {num_partitions} partitions to {full_table_name}")
        if partition_columns:
            context.log.info(f"Physical partitioning by columns: {partition_columns}")

        # Log DataFrame schema
        context.log.info(f"DataFrame schema: {[f.name for f in data.schema.fields]}")

        import time

        write_start = time.time()

        # Create write operation
        writer = (
            data.write.format("delta")
            .mode("overwrite")
            .options(**write_options)
            .partitionBy(*partition_columns)
        )

        # Log that write is starting
        context.log.info(f"⏳ Executing Delta write to {full_table_name}...")

        # Execute the write
        writer.saveAsTable(full_table_name)

        write_duration = time.time() - write_start
        context.log.info(
            f"✓ Delta write completed in {write_duration:.2f} seconds "
            f"({write_duration/60:.2f} minutes)"
        )

        # Log post-write statistics
        spark = data.sparkSession
        try:
            dt = DeltaTable.forName(spark, full_table_name)
            row_count = dt.toDF().count()
            context.log.info(f"✓ Table {full_table_name} now contains {row_count} rows")

            # Get file statistics from Delta history
            history = dt.history(1).select("operationMetrics").collect()
            if history and history[0]["operationMetrics"]:
                metrics = history[0]["operationMetrics"]
                num_files = metrics.get("numFiles", "unknown")
                num_output_bytes = metrics.get(
                    "numOutputBytes", metrics.get("numOutputRows", "unknown")
                )
                context.log.info(
                    f"✓ Write metrics: {num_files} files, " f"{num_output_bytes} bytes"
                )
        except Exception as e:
            context.log.warning(f"Could not retrieve post-write statistics: {e}")

    def _handle_truncate(
        self,
        context: OutputContext,
        output: sql.DataFrame,
        config: FileConfig,
        full_table_name: str,
    ) -> bool:
        """Handles truncation of a table's contents if conditions are met.

        Returns:
            bool: True if truncation was performed, False otherwise
        """
        truncate_schemas = [
            "school_coverage",
            "school_geolocation",
            "school_master",
            "school_reference",
        ]

        if (
            output.isEmpty()
            and config.metastore_schema in truncate_schemas
            and "adhoc" not in context.asset_key.path[-1]
        ):
            context.log.info(
                f"Output is empty, clearing all data from {full_table_name}"
            )

            empty_df = self._get_spark_session().createDataFrame([], output.schema)

            self._overwrite_data(
                empty_df, config.metastore_schema, full_table_name, context
            )

            context.log.info(f"Successfully cleared all data from {full_table_name}")
            return True
        return False
