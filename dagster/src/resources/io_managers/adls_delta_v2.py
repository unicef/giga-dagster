from pathlib import Path

from dagster_pyspark import PySparkResource
from delta import DeltaTable
from icecream import ic
from pydantic import AnyUrl
from pyspark import sql
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField

import src.schemas
from dagster import InputContext, OutputContext
from src.resources.io_managers.base import BaseConfigurableIOManager
from src.schemas import BaseSchema
from src.settings import settings
from src.utils.adls import ADLSFileClient

adls_client = ADLSFileClient()


class ADLSDeltaV2IOManager(BaseConfigurableIOManager):
    pyspark: PySparkResource

    def handle_output(self, context: OutputContext, output: sql.DataFrame):
        filepath = self._get_filepath(context)
        table_name, table_root_path, table_path = self._get_table_path(
            context, filepath
        )
        schema_name = self._get_schema_name(context)
        full_table_name = f"{schema_name}.{table_name}"
        columns = self._get_schema_object(schema_name)

        self._create_schema_if_not_exists(schema_name)
        self._create_table_if_not_exists(
            context, schema_name, table_name, columns, table_root_path
        )
        self._upsert_data(context, output, full_table_name, columns)

        context.log.info(f"Uploaded {table_name} to {table_root_path} in ADLS.")

    def load_input(self, context: InputContext) -> sql.DataFrame:
        filepath = self._get_filepath(context.upstream_output)
        table_name, table_root_path, table_path = self._get_table_path(
            context, filepath
        )
        spark = self._get_spark_session()
        schema_name = self._get_schema_name(context)
        full_table_name = f"{schema_name}.{table_name}"
        dt = DeltaTable.forName(spark, full_table_name)

        context.log.info(f"Downloaded {table_name} from {table_root_path} in ADLS.")

        return dt.toDF()

    @staticmethod
    def _get_table_path(
        context: InputContext | OutputContext, filepath: str
    ) -> tuple[str, str, AnyUrl]:
        if context.step_context.op_config["dataset_type"] == "qos":
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
    def _get_schema_name(context: InputContext | OutputContext):
        return context.step_context.op_config["metastore_schema"]

    def _get_schema_object(self, schema_name: str):
        # TODO: Pull the correct Delta Table schema from ADLS
        schema: BaseSchema = getattr(src.schemas, schema_name)
        return schema.columns

    def _get_spark_session(self) -> SparkSession:
        spark: PySparkResource = self.pyspark
        s: SparkSession = spark.spark_session
        return s

    def _create_schema_if_not_exists(self, schema_name: str):
        spark = self._get_spark_session()
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema_name}`").show()

    def _create_table_if_not_exists(
        self,
        context: OutputContext,
        schema_name: str,
        table_name: str,
        columns: list[StructField],
        table_root_path: str = None,
    ):
        spark = self._get_spark_session()
        full_table_name = f"{schema_name}.{table_name}"

        query = (
            DeltaTable.createIfNotExists(spark)
            .tableName(full_table_name)
            .addColumns(columns)
        )

        if (
            len(
                partition_columns := context.step_context.op_config["partition_columns"]
            )
            > 0
        ):
            query.partitionedBy(*partition_columns)

        query = query.property("delta.enableChangeDataFeed", "true")

        try:
            query.execute()
        except AnalysisException as exc:
            if "DELTA_TABLE_NOT_FOUND" in str(exc):
                # This error happens when you delete the Delta Table in ADLS
                # Its corresponding entry in the metastore needs to be dropped
                spark.sql(f"DROP TABLE `{schema_name}`.`{table_name.lower()}`").show()
                query.location(
                    f"{settings.SPARK_WAREHOUSE_DIR}/{schema_name}.db/{table_name.lower()}"
                ).execute()
            else:
                raise exc

    def _upsert_data(
        self,
        context: OutputContext,
        data: sql.DataFrame,
        full_table_name: str,
        columns: list[StructField],
    ):
        spark = self._get_spark_session()
        unique_identifier_column = context.step_context.op_config[
            "unique_identifier_column"
        ]
        (
            DeltaTable.forName(spark, full_table_name)
            .alias("master")
            .merge(
                data.alias("updates"),
                f"master.{unique_identifier_column} = updates.{unique_identifier_column}",
            )
            .whenMatchedUpdate(
                "master.signature <> updates.signature",
                dict(
                    zip(
                        [c.name for c in columns],
                        [f"updates.{c.name}" for c in columns],
                        strict=True,
                    )
                ),
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
