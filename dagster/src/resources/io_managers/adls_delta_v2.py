import os.path

from dagster_pyspark import PySparkResource
from delta import DeltaTable
from pyspark import sql
from pyspark.sql import SparkSession

import src.schemas
from dagster import InputContext, OutputContext
from src.resources.io_managers.base import BaseConfigurableIOManager
from src.schemas import BaseSchema
from src.settings import settings
from src.utils.adls import ADLSFileClient

adls_client = ADLSFileClient()


class ADLSDeltaV2IOManager(BaseConfigurableIOManager):
    pyspark: PySparkResource

    @staticmethod
    def _get_table_path(
        context: InputContext | OutputContext, filepath: str
    ) -> tuple[str, str, str]:
        table_name = filepath.split("/")[-1].split("_")[0].replace("-", "_")
        table_root_path = os.path.splitext(filepath)[0]
        return (
            table_name,
            table_root_path,
            f"{settings.AZURE_BLOB_CONNECTION_URI}/{table_root_path}/{table_name}",
        )

    @staticmethod
    def _get_schema(context: InputContext | OutputContext):
        return context.step_context.op_config["metastore_schema"]

    def _get_spark_session(self) -> SparkSession:
        spark: PySparkResource = self.pyspark
        s: SparkSession = spark.spark_session
        return s

    def handle_output(self, context: OutputContext, output: sql.DataFrame):
        filepath = self._get_filepath(context)
        table_name, table_root_path, table_path = self._get_table_path(
            context, filepath
        )

        schema_name = self._get_schema(context)

        # TODO: Pull the correct Delta Table schema from ADLS
        schema: BaseSchema = getattr(src.schemas, schema_name)
        columns = schema.columns
        spark = self._get_spark_session()

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema_name}`").show()
        (
            DeltaTable.createIfNotExists(spark)
            .tableName(table_name)
            .addColumns(columns)
            .property("delta.enableChangeDataFeed", "true")
            .location(table_path)
            .execute()
        )
        dt = DeltaTable.forName(spark, table_name)
        (
            dt.alias("master")
            .merge(
                output.alias("updates"),
                "master.school_id_giga = updates.school_id_giga",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        context.log.info(f"Uploaded {table_name} to {table_root_path} in ADLS.")

    def load_input(self, context: InputContext) -> sql.DataFrame:
        filepath = self._get_filepath(context.upstream_output)
        table_name, table_root_path, table_path = self._get_table_path(
            context, filepath
        )
        spark = self._get_spark_session()
        dt = DeltaTable.forName(spark, table_name)

        context.log.info(f"Downloaded {table_name} from {table_root_path} in ADLS.")

        return dt.toDF()
