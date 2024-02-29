from dagster_pyspark import PySparkResource
from pyspark import sql

from dagster import InputContext, OutputContext
from src.utils.adls import ADLSFileClient

from .base import BaseConfigurableIOManager

adls_client = ADLSFileClient()


class AdlsCsvIOManager(BaseConfigurableIOManager):
    pyspark: PySparkResource
    engine: str

    def handle_output(self, context: OutputContext, output: sql.DataFrame):
        filepath = self._get_filepath(context)
        if output.isEmpty():
            context.log.warning("Output DataFrame is empty.")

        match self.engine:
            case "spark":
                adls_client.upload_spark_dataframe_as_file(
                    output, filepath, self.pyspark.spark_session
                )
            case "pandas":
                adls_client.upload_pandas_dataframe_as_file(output, filepath)
            case _:
                raise ValueError(f"Unsupported IO manager engine: {self.engine}")

        context.log.info(
            f"Uploaded {filepath.split('/')[-1]} to"
            f" {'/'.join(filepath.split('/')[:-1])} in ADLS."
        )

    def load_input(self, context: InputContext) -> sql.DataFrame:
        filepath = self._get_filepath(context.upstream_output)

        match self.engine:
            case "spark":
                data = adls_client.download_csv_as_spark_dataframe(
                    filepath, self.pyspark.spark_session
                )
            case "pandas":
                data = adls_client.download_csv_as_pandas_dataframe(filepath)
            case _:
                raise ValueError(f"Unsupported IO manager engine: {self.engine}")

        context.log.info(
            f"Downloaded {filepath.split('/')[-1]} from"
            f" {'/'.join(filepath.split('/')[:-1])} in ADLS."
        )
        return data
