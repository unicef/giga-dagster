from dagster_pyspark import PySparkResource
from delta import DeltaTable
from icecream import ic
from pydantic import AnyUrl
from pyspark import sql
from pyspark.sql import SparkSession

from dagster import InputContext, OutputContext
from src.resources.io_managers.base import BaseConfigurableIOManager
from src.settings import settings
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
        if output is None:
            return

        config = FileConfig(**context.step_context.op_config)
        table_name, _, _ = self._get_table_path(context)
        schema_tier_name = construct_schema_name_for_tier(
            config.metastore_schema, config.tier
        )
        full_table_name = f"{schema_tier_name}.{table_name}"
        self._create_schema_if_not_exists(schema_tier_name)
        self._create_table_if_not_exists(context, output, schema_tier_name, table_name)
        self._upsert_data(output, config.metastore_schema, full_table_name)

        context.log.info(
            f"Uploaded {table_name} to {settings.SPARK_WAREHOUSE_DIR}/{schema_tier_name}.db in ADLS.",
        )

    def load_input(self, context: InputContext) -> sql.DataFrame:
        table_name, _, _ = self._get_table_path(context)
        spark = self._get_spark_session()
        config = FileConfig(
            **context.upstream_output.step_context.op_config
        )  # this is a scam -- gives the current asset step's config, not the upstream asset
        schema_name = config.metastore_schema

        schema_tier_name = construct_schema_name_for_tier(schema_name, config.tier)
        full_table_name = construct_full_table_name(schema_tier_name, table_name)

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
        table_name = config.country_code
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

        if schema_name_for_tier == "qos":
            columns = data.schema.fields
            partition_columns = [
                FileConfig(**context.step_context.op_config).row_data_dict[
                    "response_date_key"
                ]
            ]
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
        execute_query_with_error_handler(
            spark, query, schema_name_for_tier, table_name, context
        )

    def _upsert_data(
        self,
        data: sql.DataFrame,
        schema_name: str,
        full_table_name: str,
    ):
        spark = self._get_spark_session()

        if schema_name == "qos":
            gold_schema = DeltaTable.forName(spark, full_table_name).toDF().schema
            incoming_schema = data.schema
            gold_columns = {col.name for col in gold_schema.fields}
            gold_columns.discard("signature")
            incoming_columns = {col.name for col in incoming_schema.fields}

            if len(gold_columns.symmetric_difference(incoming_columns)) > 0:
                dummy_df = spark.createDataFrame([], schema=incoming_schema)
                dummy_df.write.format("delta").option("mergeSchema", "true").mode(
                    "append"
                ).saveAsTable(full_table_name)

            columns = incoming_schema.fields
            primary_key = "gigasync_id"
        else:
            columns = get_schema_columns(spark, schema_name)
            primary_key = get_primary_key(spark, schema_name)

        update_columns = [c.name for c in columns if c.name != primary_key]
        master = DeltaTable.forName(spark, full_table_name)
        query = build_deduped_merge_query(master, data, primary_key, update_columns)

        if query is not None:
            query.execute()
