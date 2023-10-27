from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.settings import (
    AZURE_SAS_TOKEN,
    AZURE_STORAGE_ACCOUNT_NAME,
    SPARK_RPC_AUTHENTICATION_SECRET,
)

builder = (
    SparkSession.builder.master("spark://spark-master:7077")
    .appName("GigaDagsterSpark")
    .config(
        map={
            "spark.jars.packages": ",".join(
                [
                    "org.apache.hadoop:hadoop-azure:3.3.6",
                    "com.microsoft.azure:azure-storage:8.6.6",
                ]
            ),
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": (
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            ),
            "spark.authenticate": True,
            "spark.authenticate.secret": SPARK_RPC_AUTHENTICATION_SECRET,
            "spark.authenticate.enableSaslEncryption": True,
        }
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.conf.set(
    f"fs.azure.account.auth.type.{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
    "SharedKey",
)
spark.conf.set(
    f"fs.azure.account.key.{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
    AZURE_SAS_TOKEN,
)
