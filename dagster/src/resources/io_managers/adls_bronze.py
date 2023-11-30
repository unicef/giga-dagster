import pandas as pd
from dagster_pyspark import PySparkResource

from dagster import InputContext, OutputContext
from src.utils.adls import ADLSFileClient

from .base import BaseConfigurableIOManager

adls_client = ADLSFileClient()


class ADLSBronzeIOManager(BaseConfigurableIOManager):
    pyspark: PySparkResource

    def load_input(self, context: InputContext) -> pd.DataFrame:
        filepath = self._get_filepath(context.upstream_output)
        file = adls_client.download_csv_as_spark_dataframe(
            filepath, self.pyspark.spark_session
        )
        context.log.info(
            f"Downloaded {filepath.split('/')[-1]} from"
            f" {'/'.join(filepath.split('/')[:-1])} in ADLS."
        )
        return file

    def handle_output(self, context: OutputContext, output: pd.DataFrame):
        filepath = self._get_filepath(context)
        if output.empty:
            context.log.warning("Output DataFrame is empty. Skipping write operation.")
            return

        adls_client.upload_pandas_dataframe_as_file(filepath, output)
        context.log.info(
            f"Uploaded {filepath.split('/')[-1]} to"
            f" {'/'.join(filepath.split('/')[:-1])} in ADLS."
        )
