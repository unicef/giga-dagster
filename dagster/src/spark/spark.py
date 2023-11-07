from delta import configure_spark_with_delta_pip
from pyspark import SparkConf
from pyspark.sql import SparkSession

from src.settings import (
    AZURE_BLOB_CONTAINER_NAME,
    AZURE_SAS_TOKEN,
    AZURE_STORAGE_ACCOUNT_NAME,
    SPARK_RPC_AUTHENTICATION_SECRET,
)


def get_spark_session():
    conf = SparkConf()
    conf.set(
        "spark.driver.extraJavaOptions",
        " ".join(
            [
                "-Divy.cache.dir=/tmp",
                "-Divy.home=/tmp",
                "-Dio.netty.tryReflectionSetAccessible=true",
            ]
        ),
    )
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    conf.set("spark.authenticate", "true")
    conf.set("spark.authenticate.secret", SPARK_RPC_AUTHENTICATION_SECRET)
    conf.set("spark.authenticate.enableSaslEncryption", "true")
    conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
    conf.set(
        f"fs.azure.sas.{AZURE_BLOB_CONTAINER_NAME}.{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
        AZURE_SAS_TOKEN,
    )

    builder = (
        SparkSession.builder.master("spark://spark-master:7077")
        .appName("GigaDagsterSpark")
        .config(conf=conf)
    )
    spark = configure_spark_with_delta_pip(builder)
    return spark.getOrCreate()
