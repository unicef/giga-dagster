from dagster_pyspark import PySparkResource
from pyspark.sql import DataFrame

from dagster import ConfigurableIOManager, InputContext, OutputContext
from src._utils.adls import ADLSFileClient, _get_filepath
from src._utils.spark import transform_dataframe_for_deltatable

adls_client = ADLSFileClient()


class StagingADLSIOManager(ConfigurableIOManager):
    pyspark: PySparkResource

    def handle_output(self, context: OutputContext, output: DataFrame):
        filepath = self._get_filepath(context)
        if context.step_key == "data_quality_results":
            adls_client.upload_json_to_adls_json(filepath, output)
            return
        else:
            if context.step_key == "raw":
                if output.empty():
                    context.log.warning(
                        "Output DataFrame is empty. Skipping write operation."
                    )
                    return
                adls_client.upload_pandas_dataframe_to_adls_csv(
                    context, filepath, output
                )
            elif context.step_key == "bronze":
                if output.isEmpty():
                    context.log.warning(
                        "Output DataFrame is empty. Skipping write operation."
                    )
                    return
                adls_client.upload_spark_dataframe_to_adls_csv(
                    output, filepath, self.pyspark.spark_session
                )
            else:
                output = transform_dataframe_for_deltatable(context, output)
                adls_client.upload_spark_dataframe_to_adls_deltatable(
                    output, filepath, self.pyspark.spark_session
                )

        context.log.info(
            f"Uploaded {filepath.split('/')[-1]} to"
            f" {('/').join(filepath.split('/')[:-1])} in ADLS."
        )

    def load_input(self, context: InputContext):
        filepath = self._get_filepath(context.upstream_output)

        if (
            context.upstream_output.step_key == "data_quality_results"
            and context.asset_key.to_user_string() == "data_quality_results"
        ):
            file = adls_client.download_adls_json_to_json(filepath)
        elif context.upstream_output.step_key == "raw":
            file = adls_client.download_adls_csv_to_pandas(filepath)
        elif context.upstream_output.step_key == "bronze":
            file = adls_client.download_adls_csv_to_spark_dataframe(
                filepath, self.pyspark.spark_session
            )
        else:
            file = adls_client.download_adls_deltatable_to_spark_dataframe(
                filepath, self.pyspark.spark_session
            )

        context.log.info(
            f"Downloaded {filepath.split('/')[-1]} from"
            f" {('/').join(filepath.split('/')[:-1])} in ADLS."
        )

        return file

    def _get_filepath(self, context):
        filepath = context.step_context.op_config["filepath"]

        parent_folder = context.step_context.op_config["dataset_type"]
        step = context.step_key

        destination_filepath = _get_filepath(filepath, parent_folder, step)

        context.log.info(f"Moving from {filepath} to {destination_filepath}")

        return destination_filepath
