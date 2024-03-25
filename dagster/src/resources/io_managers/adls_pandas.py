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
        path = self._get_filepath(context)

        if output.empty:
            context.log.warning("Output DataFrame is empty.")

        adls_client.upload_pandas_dataframe_as_file(
            context=context, data=output, filepath=str(path)
        )

        context.log.info(f"Uploaded {path.name} to {path.parent} in ADLS.")

        context.log.info(
            f"EMIT LINEAGE CALLED FROM IO MANAGER: {self.__class__.__name__}"
        )
        emit_lineage(context=context)

    def load_input(self, context: InputContext) -> sql.DataFrame:
        path = self._get_filepath(context)

        data = adls_client.download_csv_as_spark_dataframe(
            str(path), self.pyspark.spark_session
        )

        context.log.info(f"Downloaded {path.name} from {path.parent} in ADLS.")

        return data
