import pandas as pd
from dagster_pyspark import PySparkResource
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from azure.core.exceptions import ResourceNotFoundError
from dagster import InputContext, OutputContext
from src.utils.adls import ADLSFileClient

from .base import BaseConfigurableIOManager

adls_client = ADLSFileClient()


class ADLSPandasIOManager(BaseConfigurableIOManager):
    pyspark: PySparkResource

    def handle_output(self, context: OutputContext, output: pd.DataFrame):
        path = self._get_filepath(context)

        if output.empty:
            context.log.warning("Output DataFrame is empty.")

        adls_client.upload_pandas_dataframe_as_file(
            context=context,
            data=output,
            filepath=str(path),
        )

        context.log.info(f"Uploaded {path.name} to {path.parent} in ADLS.")

    def load_input(self, context: InputContext) -> sql.DataFrame | None:
        spark: SparkSession = self.pyspark.spark_session
        path = self._get_filepath(context)

        try:
            data = adls_client.download_csv_as_spark_dataframe(str(path), spark)
        except ResourceNotFoundError as e:
            if "_reference_" in context.asset_key.to_user_string():
                # Required so that the asset does not skip materialization
                # which also causes all downstream assets to skip
                context.log.warning(f"Reference file {path!s} does not exist.")
                return spark.createDataFrame([], StructType())
            raise e

        context.log.info(f"Downloaded {path.name} from {path.parent} in ADLS.")
        return data
