from pathlib import Path

from dagster_pyspark import PySparkResource
from delta import DeltaTable
from icecream import ic
from pydantic import AnyUrl
from pyspark import sql
from pyspark.errors.exceptions.captured import AnalysisException
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
    ) -> tuple[str, str, AnyUrl]:
        if context.step_context.op_config.get("dataset_type") == "qos":
            table_name = Path(context.step_context.op_config["filepath"]).parent.name
        else:
            table_name = Path(filepath).name.split("_")[0]

        table_root_path = "/".join(filepath.split("/")[:-1])
        return (
            ic(table_name),
            ic(table_root_path),
            ic(f"{settings.AZURE_BLOB_CONNECTION_URI}/{table_root_path}/{table_name}"),
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
        full_table_name = f"{schema_name}.{table_name}"
        unique_identifier_column = context.step_context.op_config.get(
            "unique_identifier_column", "school_id_giga"
        )

        # TODO: Pull the correct Delta Table schema from ADLS
        schema: BaseSchema = getattr(src.schemas, schema_name)
        columns = schema.columns
        spark = self._get_spark_session()

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema_name}`").show()
        query = (
            DeltaTable.createOrReplace(spark)
            .tableName(full_table_name)
            .addColumns(columns)
        )

        if (
            len(
                partition_columns := context.step_context.op_config.get(
                    "partition_columns", []
                )
            )
            > 0
        ):
            query.partitionedBy(*partition_columns)

        query = query.property("delta.enableChangeDataFeed", "true")

        try:
            query.execute()
        except AnalysisException as exc:
            if "DELTA_TABLE_NOT_FOUND" in str(exc):
                query.location(
                    f"{settings.SPARK_WAREHOUSE_DIR}/{schema_name}.db/{table_name.lower()}"
                ).execute()
            else:
                raise exc

        (
            DeltaTable.forName(spark, full_table_name)
            .alias("master")
            .merge(
                output.alias("updates"),
                f"master.{unique_identifier_column} = updates.{unique_identifier_column}",
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
