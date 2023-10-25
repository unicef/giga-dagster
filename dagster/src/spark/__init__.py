from pyspark.sql import SparkSession

from src.settings import AZURE_SAS_TOKEN, AZURE_STORAGE_ACCOUNT_NAME

spark = (
    SparkSession.builder.remote("sc://spark-master:7077")
    .appName("GigaDagsterSpark")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

spark.conf.set(
    f"fs.azure.account.auth.type.{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
    "SharedKey",
)
spark.conf.set(
    f"fs.azure.account.key.{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
    AZURE_SAS_TOKEN,
)
