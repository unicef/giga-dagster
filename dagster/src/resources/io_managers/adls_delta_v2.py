from pathlib import Path

from dagster_pyspark import PySparkResource
from delta import DeltaTable
from icecream import ic
from models.schema import Schema
from pydantic import AnyUrl
from pyspark import sql
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructField

from dagster import InputContext, OutputContext
from src.constants import constants
from src.resources.io_managers.base import BaseConfigurableIOManager
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

        self._create_schema_if_not_exists(schema_name)
        self._create_table_if_not_exists(schema_name, table_name)
        self._upsert_data(output, schema_name, full_table_name)

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

    def _get_schema_table(self, schema_name: str):
        spark = self._get_spark_session()
        metaschema_name = Schema.__schema_name__
        full_table_name = f"{metaschema_name}.{schema_name}"

        # This should be cheap if the migrations.migrate_schema asset is caching the table properly
        return DeltaTable.forName(spark, full_table_name).toDF()

    def _get_schema_object(self, schema_name: str):
        df = self._get_schema_table(schema_name)
        return [
            StructField(
                row.name,
                getattr(constants.TYPE_MAPPINGS, row.data_type).pyspark(),
                row.is_nullable,
            )
            for row in df.collect()
        ]

    def _get_primary_key(self, schema_name: str) -> str:
        df = self._get_schema_table(schema_name)
        return df.filter(df["primary_key"]).first().name

    def _get_partition_columns(self, schema_name: str) -> list[str]:
        df = self._get_schema_table(schema_name)
        return [
            row.name
            for row in df.filter(
                col("partition_order").isNotNull() & (col("partition_order") > 0)
            )
            .select("name")
            .collect()
        ]

    def _get_spark_session(self) -> SparkSession:
        spark: PySparkResource = self.pyspark
        s: SparkSession = spark.spark_session
        return s

    def _create_schema_if_not_exists(self, schema_name: str):
        spark = self._get_spark_session()
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema_name}`").show()

    def _create_table_if_not_exists(
        self,
        schema_name: str,
        table_name: str,
    ):
        spark = self._get_spark_session()
        full_table_name = f"{schema_name}.{table_name}"
        columns = self._get_schema_object(schema_name)
        partition_columns = self._get_partition_columns(schema_name)

        query = (
            DeltaTable.createIfNotExists(spark)
            .tableName(full_table_name)
            .addColumns(columns)
        )

        if len(partition_columns) > 0:
            query.partitionedBy(*partition_columns)

        query = query.property("delta.enableChangeDataFeed", "true")

        try:
            query.execute()
        except AnalysisException as exc:
            if "DELTA_TABLE_NOT_FOUND" in str(exc):
                # This error gets raised when you delete the Delta Table in ADLS and subsequently try to re-ingest the
                # same table. Its corresponding entry in the metastore needs to be dropped first.
                #
                # Deleting a table in ADLS does not drop its metastore entry; the inverse is also true.
                spark.sql(f"DROP TABLE `{schema_name}`.`{table_name.lower()}`").show()
                query.location(
                    f"{settings.SPARK_WAREHOUSE_DIR}/{schema_name}.db/{table_name.lower()}"
                ).execute()
            else:
                raise exc

    def _upsert_data(
        self,
        data: sql.DataFrame,
        schema_name: str,
        full_table_name: str,
    ):
        spark = self._get_spark_session()
        columns = self._get_schema_object(schema_name)
        primary_key = self._get_primary_key(schema_name)

        (
            DeltaTable.forName(spark, full_table_name)
            .alias("master")
            .merge(
                data.alias("updates"),
                f"master.{primary_key} = updates.{primary_key}",
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
