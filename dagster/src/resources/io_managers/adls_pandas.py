import pandas as pd
from dagster_pyspark import PySparkResource
from pyspark import sql

from dagster import InputContext, OutputContext
from src.utils.adls import ADLSFileClient
from src.utils.datahub.emit_dataset_metadata import emit_metadata_to_datahub

from .base import BaseConfigurableIOManager

adls_client = ADLSFileClient()


class ADLSPandasIOManager(BaseConfigurableIOManager):
    pyspark: PySparkResource

    def handle_output(self, context: OutputContext, output: pd.DataFrame):
        filepath = self._get_filepath(context)
        if output.empty:
            context.log.warning("Output DataFrame is empty.")
        #     return

        adls_client.upload_pandas_dataframe_as_file(output, filepath)

        context.log.info(
            f"Uploaded {filepath.split('/')[-1]} to"
            f" {'/'.join(filepath.split('/')[:-1])} in ADLS."
        )

        context.log.info("EMITTING METADATA TO DATAHUB")
        input_filepath = context.step_context.op_config["filepath"]
        context.log.info(f"Metadata Input Filepath: {input_filepath}")
        context.log.info(f"Metadata Output Filepath: {filepath}")
        emit_metadata_to_datahub(
            context,
            output_filepath=filepath,
            input_filepath=input_filepath,
            df=output,
            data_format="csv",
        )

        context.log.info(
            f"Metadata of {filepath.split('/')[-1]} has been successfully emitted to Datahub."
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
