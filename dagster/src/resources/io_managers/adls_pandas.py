import datahub.emitter.mce_builder as builder
import pandas as pd
from dagster_pyspark import PySparkResource
from pyspark import sql

from dagster import InputContext, OutputContext
from src.utils.adls import ADLSFileClient
from src.utils.datahub.emit_lineage import emit_lineage

from .base import BaseConfigurableIOManager

adls_client = ADLSFileClient()


class ADLSPandasIOManager(BaseConfigurableIOManager):
    pyspark: PySparkResource

    def handle_output(self, context: OutputContext, output: pd.DataFrame):
        filepath = self._get_filepath(context)
        if output.empty:
            context.log.warning("Output DataFrame is empty.")
        #     return

        adls_client.upload_pandas_dataframe_as_file(
            context=context, data=output, filepath=filepath
        )

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

        current_filepath = self._get_filepath_from_InputContext(context)
        context.log.info(f"current_filepath: {current_filepath}")
        platform = builder.make_data_platform_urn("adlsGen2")
        emit_lineage(
            context,
            dataset_filepath=current_filepath,
            upstream_filepath=filepath,
            platform=platform,
        )

        return data
