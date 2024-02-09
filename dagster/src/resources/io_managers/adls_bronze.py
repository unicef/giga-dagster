from dagster_pyspark import PySparkResource
from pyspark import sql

from dagster import InputContext, OutputContext
from src.utils.adls import ADLSFileClient

from .base import BaseConfigurableIOManager

adls_client = ADLSFileClient()


class ADLSBronzeIOManager(BaseConfigurableIOManager):
    pyspark: PySparkResource

    def handle_output(self, context: OutputContext, output: sql.DataFrame):
        filepath = self._get_filepath(context)
        context.log.info(f">>BRONZE LOADOUTPUT RAN, {filepath}")
        if output.isEmpty():
            context.log.warning("Output DataFrame is empty. Skipping write operation.")
            return

        adls_client.upload_pandas_dataframe_as_file(output.toPandas(), filepath)
        context.log.info(
            f"Uploaded {filepath.split('/')[-1]} to"
            f" {'/'.join(filepath.split('/')[:-1])} in ADLS."
        )

    def load_input(self, context: InputContext) -> sql.DataFrame:
        filepath = self._get_filepath(context.upstream_output)
        file = adls_client.download_csv_as_spark_dataframe(
            filepath, self.pyspark.spark_session
        )
        context.log.info(">>BRONZE LOADINPUT RAN")
        context.log.info(
            f"Downloaded {filepath.split('/')[-1]} from"
            f" {'/'.join(filepath.split('/')[:-1])} in ADLS."
        )
        return file
