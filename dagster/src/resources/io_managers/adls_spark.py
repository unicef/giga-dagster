import time

import pandas as pd
from dagster_pyspark import PySparkResource
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.types import NullType, StringType

from azure.core.exceptions import ResourceNotFoundError
from dagster import InputContext, OutputContext
from src.settings import settings
from src.utils.adls import ADLSFileClient

from .base import BaseConfigurableIOManager

adls_client = ADLSFileClient()


class ADLSSparkIOManager(BaseConfigurableIOManager):
    """Writes Spark DataFrames natively to ADLS (parquet or csv directory).

    Uses a cache → isEmpty → write → unpersist pattern so the plan executes
    once and executor memory is freed immediately after the write.
    """

    pyspark: PySparkResource

    def handle_output(self, context: OutputContext, output: sql.DataFrame):
        path = self._get_filepath(context)
        adls_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{path}"

        # Cast NullType columns to StringType — schema-only, no action triggered yet
        for field in output.schema.fields:
            if isinstance(field.dataType, NullType):
                output = output.withColumn(
                    field.name, output[field.name].cast(StringType())
                )

        # cache → isEmpty (populates cache) → write (reads from cache) → unpersist
        t0 = time.time()
        output.cache()
        output.isEmpty()
        context.log.info(f"Cache populated in {time.time() - t0:.2f}s")

        t1 = time.time()
        match path.suffix:
            case ".parquet":
                output.write.mode("overwrite").parquet(adls_path)
            case ".csv":
                output.write.mode("overwrite").csv(adls_path, header=True)
            case _:
                raise OSError(f"Unsupported format for Spark write: {path.suffix}")
        context.log.info(f"Write to ADLS completed in {time.time() - t1:.2f}s")

        output.unpersist()
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
                    # No native Spark Excel support — bridge via pandas
                    pdf = pd.read_excel(adls_path)
                    data = spark.createDataFrame(pdf.astype(str))
                case _:
                    raise OSError(f"Unsupported format for Spark read: {path.suffix}")
        except ResourceNotFoundError as e:
            raise e

        context.log.info(f"Downloaded {path.name} from {path.parent} in ADLS.")
        return data
