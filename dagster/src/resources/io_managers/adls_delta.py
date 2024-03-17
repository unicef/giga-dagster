from dagster_pyspark import PySparkResource
from pyspark import sql

from dagster import InputContext, OutputContext
from src.resources.io_managers.base import BaseConfigurableIOManager
from src.utils.adls import ADLSFileClient

adls_client = ADLSFileClient()


class ADLSDeltaIOManager(BaseConfigurableIOManager):
    pyspark: PySparkResource

    def handle_output(self, context: OutputContext, output: sql.DataFrame):
        if context.step_key in ["staging", "silver", "gold"]:
            return

        if output.isEmpty():
            context.log.warning("Output DataFrame is empty. Skipping write operation.")
            return

        filepath = self._get_filepath(context)
        table_path = self._get_table_path(context, filepath)

        schema_name = self._get_schema_name(context)
        type_transform_function = self._get_type_transform_function(context)
        output = type_transform_function(output, context)
        adls_client.upload_spark_dataframe_as_delta_table(
            output,
            table_path,
            schema_name,
            self.pyspark.spark_session,
        )

        context.log.info(
            f"Uploaded {filepath.split('/')[-1]} to"
            f" {'/'.join(filepath.split('/')[:-1])} in ADLS."
        )

    def load_input(self, context: InputContext) -> sql.DataFrame:
        filepath = self._get_filepath(context)
        table_path = self._get_table_path(context.upstream_output, filepath)

        data = adls_client.download_delta_table_as_spark_dataframe(
            table_path, self.pyspark.spark_session
        )

        context.log.info(
            f"Downloaded {filepath.split('/')[-1]} from"
            f" {'/'.join(filepath.split('/')[:-1])} in ADLS."
        )

        return data
