import pandas as pd
from pyspark import sql

from dagster import InputContext, OutputContext
from src.utils.adls import ADLSFileClient

from .base import BaseConfigurableIOManager

adls_client = ADLSFileClient()


class ADLSPandasIOManager(BaseConfigurableIOManager):
    def handle_output(self, context: OutputContext, output: pd.DataFrame):
        filepath = self._get_filepath(context)
        if output.empty:
            context.log.warning("Output DataFrame is empty. Skipping write operation.")
            return

        adls_client.upload_pandas_dataframe_as_file(output, filepath)

        context.log.info(
            f"Uploaded {filepath.split('/')[-1]} to"
            f" {'/'.join(filepath.split('/')[:-1])} in ADLS."
        )

    def load_input(self, context: InputContext) -> sql.DataFrame:
        filepath = self._get_filepath(context.upstream_output)
        data = adls_client.download_csv_as_spark_dataframe(
            filepath, self.pyspark.spark_session
        )
        context.log.info(
            f"Downloaded {filepath.split('/')[-1]} from"
            f" {'/'.join(filepath.split('/')[:-1])} in ADLS."
        )
        return data
