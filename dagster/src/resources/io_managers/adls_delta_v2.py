from pathlib import Path

from dagster_pyspark import PySparkResource
from delta import DeltaTable
from icecream import ic
from pydantic import AnyUrl
from pyspark import sql
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, concat_ws, sha2

from dagster import InputContext, OutputContext
from src.resources.io_managers.base import BaseConfigurableIOManager
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.schema import (
    get_partition_columns,
    get_primary_key,
    get_schema_columns,
    get_schema_name,
)

adls_client = ADLSFileClient()


class ADLSDeltaV2IOManager(BaseConfigurableIOManager):
    pyspark: PySparkResource

    def handle_output(self, context: OutputContext, output: sql.DataFrame):
        filepath = self._get_filepath(context)
        table_name, table_root_path, table_path = self._get_table_path(
            context, filepath
        )
        schema_name = get_schema_name(context)
        full_table_name = f"{schema_name}.{table_name}"

        self._create_schema_if_not_exists(schema_name)
        self._create_table_if_not_exists(context, schema_name, table_name)
        self._upsert_data(output, schema_name, full_table_name)

        context.log.info(f"Uploaded {table_name} to {table_root_path} in ADLS.")

    def load_input(self, context: InputContext) -> sql.DataFrame:
        filepath = self._get_filepath(context.upstream_output)
        table_name, table_root_path, table_path = self._get_table_path(
            context, filepath
        )
        spark = self._get_spark_session()
        schema_name = get_schema_name(context)
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
        schema_name: str,
        table_name: str,
    ):
        spark = self._get_spark_session()
        full_table_name = f"{schema_name}.{table_name}"
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
            else:
                raise exc

    def _upsert_data(
        self,
        data: sql.DataFrame,
        schema_name: str,
        full_table_name: str,
    ):
        spark = self._get_spark_session()
        columns = get_schema_columns(spark, schema_name)
        primary_key = get_primary_key(spark, schema_name)

        master = DeltaTable.forName(spark, full_table_name).alias("master")
        master_df = master.toDF()
        incoming = data.alias("incoming")

        master_ids = master_df.select(primary_key, "signature")
        incoming_ids = incoming.select(primary_key, "signature")

        updates_df = incoming_ids.join(master_ids, primary_key, "inner")
        inserts_df = incoming_ids.join(master_ids, primary_key, "left_anti")

        updates_signature = updates_df.agg(
            sha2(concat_ws("|", collect_list("incoming.signature")), 256).alias(
                "combined_signature"
            )
        ).first()["combined_signature"]
        master_to_update_signature = master_ids.agg(
            sha2(concat_ws("|", collect_list("signature")), 256).alias(
                "combined_signature"
            )
        ).first()["combined_signature"]

        has_updates = master_to_update_signature != updates_signature
        has_insertions = inserts_df.count() > 0

        if not (has_updates or has_insertions):
            return

        query = master.merge(incoming, f"master.{primary_key} = incoming.{primary_key}")

        if has_updates:
            query = query.whenMatchedUpdate(
                "master.signature <> incoming.signature",
                dict(
                    zip(
                        [c.name for c in columns],
                        [f"incoming.{c.name}" for c in columns],
                        strict=True,
                    )
                ),
            )
        if has_insertions:
            query = query.whenNotMatchedInsertAll()

        query.execute()
