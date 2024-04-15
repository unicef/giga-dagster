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
    get_schema_name,
)

adls_client = ADLSFileClient()


class ADLSDeltaIOManager(BaseConfigurableIOManager):
    pyspark: PySparkResource

    def handle_output(self, context: OutputContext, output: sql.DataFrame):
        if context.step_key in ["silver", "master", "reference"]:
            return

        path = self._get_filepath(context)
        table_name, _, _ = self._get_table_path(context, str(path))
        schema_name = get_schema_name(context)
        full_table_name = f"{schema_name}.{table_name}"

        self._create_schema_if_not_exists(schema_name)
        self._create_table_if_not_exists(context, output, schema_name, table_name)
        self._upsert_data(output, schema_name, full_table_name)

        context.log.info(
            f"Uploaded {table_name} to {settings.SPARK_WAREHOUSE_DIR}/{schema_name}.db in ADLS."
        )

    def load_input(self, context: InputContext) -> sql.DataFrame:
        context.log.info(f"Loading input from {context.upstream_output}")
        if context.upstream_output.step_key in ["silver"]:
            return None

        path = self._get_filepath(context)
        table_name, _, _ = self._get_table_path(context, str(path))
        spark = self._get_spark_session()
        config = FileConfig(**context.upstream_output.step_context.op_config)
        schema_name = config.metastore_schema

        schema_tier_name = construct_schema_name_for_tier(schema_name, config.tier)
        full_table_name = construct_full_table_name(schema_tier_name, table_name)

        dt = DeltaTable.forName(spark, full_table_name)

        context.log.info(
            f"Downloaded {table_name} from {settings.SPARK_WAREHOUSE_DIR}/{schema_name}.db in ADLS."
        )

        return dt.toDF()

    @staticmethod
    def _get_table_path(
        context: InputContext | OutputContext, filepath: str
    ) -> tuple[str, str, AnyUrl]:
        config = FileConfig(**context.step_context.op_config)
        table_name = config.filename_components.country_code
        table_root_path = str(config.filepath_object.parent)
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
        schema_name: str,
        table_name: str,
    ):
        spark = self._get_spark_session()
        config = FileConfig(**context.step_context.op_config)
        schema_tier_name = construct_schema_name_for_tier(schema_name, config.tier)
        full_table_name = construct_full_table_name(schema_tier_name, table_name)

        if schema_name == "qos":
            columns = data.schema.fields
            partition_columns = ["date"]
        else:
            columns = get_schema_columns(spark, schema_name)
            partition_columns = get_partition_columns(spark, schema_name)

        query = (
            DeltaTable.createIfNotExists(spark)
            .tableName(full_table_name)
            .addColumns(columns)
        )

        if len(partition_columns) > 0:
            query.partitionedBy(*partition_columns)

        query = query.property("delta.enableChangeDataFeed", "true")
        execute_query_with_error_handler(spark, query, schema_name, table_name, context)

    def _upsert_data(
        self,
        data: sql.DataFrame,
        schema_name: str,
        full_table_name: str,
    ):
        spark = self._get_spark_session()

        if schema_name == "qos":
            columns = data.schema.fields
            primary_key = "gigasync_id"
        else:
            columns = get_schema_columns(spark, schema_name)
            primary_key = get_primary_key(spark, schema_name)

        update_columns = [c.name for c in columns if c.name != primary_key]
        master = DeltaTable.forName(spark, full_table_name)
        query = build_deduped_merge_query(master, data, primary_key, update_columns)

        if query is not None:
            query.execute()
