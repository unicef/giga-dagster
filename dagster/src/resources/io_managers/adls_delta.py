from dagster_pyspark import PySparkResource
from pyspark import sql

from dagster import InputContext, OutputContext
from src.resources.io_managers.base import BaseConfigurableIOManager
from src.utils.adls import ADLSFileClient
from src.utils.datahub.emit_lineage import emit_lineage

adls_client = ADLSFileClient()


class ADLSDeltaIOManager(BaseConfigurableIOManager):
    pyspark: PySparkResource

    def handle_output(self, context: OutputContext, output: sql.DataFrame | None):
        if output is None:
            return

        if context.step_key in ["staging", "silver", "gold"]:
            return

        if output.isEmpty():
            context.log.warning("Output DataFrame is empty. Skipping write operation.")
            return

        filepath = self._get_filepath(context)
        table_path = self._get_table_path(context, str(filepath))

        schema_name = self._get_schema_name(context)
        type_transform_function = self._get_type_transform_function(context)
        output = type_transform_function(output, context)
        adls_client.upload_spark_dataframe_as_delta_table(
            output, schema_name, table_path, self.pyspark.spark_session
        )

        context.log.info(
            f"Uploaded {filepath.name} to {'/'.join(str(filepath.parent))} in ADLS."
        )

        context.log.info(
            f"EMIT LINEAGE CALLED FROM IO MANAGER: {self.__class__.__name__}"
        )
        emit_lineage(context=context)

    def load_input(self, context: InputContext) -> sql.DataFrame:
        filepath = self._get_filepath(context)
        table_path = self._get_table_path(context.upstream_output, filepath)

        data = adls_client.download_delta_table_as_spark_dataframe(
            table_path, self.pyspark.spark_session
        )

        context.log.info(
            f"Downloaded {filepath.name} from {str(filepath.parent)} in ADLS."
        )

        return data
