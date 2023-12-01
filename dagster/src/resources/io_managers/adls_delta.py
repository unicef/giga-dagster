from dagster_pyspark import PySparkResource
from pyspark.sql import DataFrame

from dagster import InputContext, OutputContext
from src.resources.io_managers.base import BaseConfigurableIOManager
from src.utils.adls import ADLSFileClient
from src.utils.spark import transform_dataframe_for_deltatable

adls_client = ADLSFileClient()


class ADLSDeltaIOManager(BaseConfigurableIOManager):
    pyspark: PySparkResource

    def handle_output(self, context: OutputContext, output: DataFrame):
        filepath = self._get_filepath(context)
        if context.step_key == "data_quality_results":
            adls_client.upload_json(filepath, output)
            return
        else:
            output = transform_dataframe_for_deltatable(context, output)
            adls_client.upload_spark_dataframe_as_delta_table(
                output, filepath, self.pyspark.spark_session
            )

        context.log.info(
            f"Uploaded {filepath.split('/')[-1]} to"
            f" {'/'.join(filepath.split('/')[:-1])} in ADLS."
        )

    def load_input(self, context: InputContext):
        filepath = self._get_filepath(context.upstream_output)

        if (
            context.upstream_output.step_key == "data_quality_results"
            and context.asset_key.to_user_string() == "data_quality_results"
        ):
            file = adls_client.download_json(filepath)
        else:
            file = adls_client.download_delta_table_as_spark_dataframe(
                filepath, self.pyspark.spark_session
            )

        context.log.info(
            f"Downloaded {filepath.split('/')[-1]} from"
            f" {'/'.join(filepath.split('/')[:-1])} in ADLS."
        )

        return file
