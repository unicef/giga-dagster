import pandas as pd
from dagster_pyspark import PySparkResource
from pyspark import sql
from pyspark.sql import SparkSession

from azure.core.exceptions import ResourceNotFoundError
from dagster import InputContext, OutputContext
from src.settings import settings
from src.utils.adls import ADLSFileClient

from .base import BaseConfigurableIOManager

adls_client = ADLSFileClient()


class ADLSSparkSingleFileIOManager(BaseConfigurableIOManager):
    """Writes Spark DataFrames as a single file to ADLS via a pandas bridge.

    Use this for human-readable output assets (CSV exports for end users) where
    a single flat file is required rather than a partitioned Spark output directory.
    The asset returns a sql.DataFrame; conversion to pandas happens here, keeping
    asset code Spark-native.

    load_input returns a sql.DataFrame so downstream assets remain Spark-native.
    """

    pyspark: PySparkResource

    def handle_output(self, context: OutputContext, output: sql.DataFrame):
        path = self._get_filepath(context)
        pdf = output.toPandas()
        adls_client.upload_pandas_dataframe_as_file(
            context=context,
            data=pdf,
            filepath=str(path),
        )
        context.log.info(f"Uploaded {path.name} to {path.parent} in ADLS.")

    def load_input(self, context: InputContext) -> sql.DataFrame:
        spark: SparkSession = self.pyspark.spark_session
        path = self._get_filepath(context)
        adls_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{path}"

        try:
            match path.suffix:
                case ".parquet":
                    data = spark.read.parquet(adls_path)
                case ".csv":
                    data = adls_client.download_csv_as_spark_dataframe(str(path), spark)
                case ".xls" | ".xlsx":
                    pdf = pd.read_excel(adls_path)
                    data = spark.createDataFrame(pdf.astype(str))
                case _:
                    raise OSError(f"Unsupported format for Spark read: {path.suffix}")
        except ResourceNotFoundError as e:
            raise e

        context.log.info(f"Downloaded {path.name} from {path.parent} in ADLS.")
        return data
