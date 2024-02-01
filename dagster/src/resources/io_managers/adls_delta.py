from dagster_pyspark import PySparkResource
from pyspark import sql

from dagster import InputContext, OutputContext
from src.resources.io_managers.base import BaseConfigurableIOManager
from src.utils.adls import ADLSFileClient

adls_client = ADLSFileClient()


class ADLSDeltaIOManager(BaseConfigurableIOManager):
    pyspark: PySparkResource

    def handle_output(self, context: OutputContext, output: sql.DataFrame):
        context.log.info(">>DELTA LOADOUTPUT RAN")

        filepath = self._get_filepath(context)
        if context.step_key == "data_quality_results":
            adls_client.upload_json(filepath, output)
            return
        else:
            if output.isEmpty():
                context.log.warning(
                    "Output DataFrame is empty. Skipping write operation."
                )
                return

            schema_name = self._get_schema(context)
            table_schema_definition = self._get_table_schema_definition(context)
            type_transform_function = self._get_type_transform_function(context)
            output = type_transform_function(output, context)
            adls_client.upload_spark_dataframe_as_delta_table(
                output,
                filepath,
                schema_name,
                table_schema_definition,
                self.pyspark.spark_session,
            )

        context.log.info(
            f"Uploaded {filepath.split('/')[-1]} to"
            f" {'/'.join(filepath.split('/')[:-1])} in ADLS."
        )

    def load_input(self, context: InputContext) -> sql.DataFrame:
        context.log.info(">>DELTA LOADINPUT RAN")

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
