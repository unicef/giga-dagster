import pyarrow_hotfix  # noqa: F401
from delta import configure_spark_with_delta_pip
from pyspark import SparkConf
from pyspark.sql import SparkSession

from src.settings import settings


def get_spark_session() -> SparkSession:
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
    conf.set("spark.sql.warehouse.dir", "/opt/spark/warehouse")
    conf.set("spark.sql.catalogImplementation", "hive")
    conf.set("spark.authenticate", "true")
    conf.set("spark.authenticate.secret", settings.SPARK_RPC_AUTHENTICATION_SECRET)
    conf.set("spark.authenticate.enableSaslEncryption", "true")
    conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
    conf.set("spark.databricks.delta.properties.defaults.appendOnly", "false")
    # conf.set("spark.python.use.daemon", "true")
    # conf.set("spark.python.daemon.module", "src.utils.sentry")
    conf.set(
        f"fs.azure.sas.{settings.AZURE_BLOB_CONTAINER_NAME}.{settings.AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
        settings.AZURE_SAS_TOKEN,
    )

    builder = (
        SparkSession.builder.master("spark://spark-master:7077")
        .appName("GigaDagsterSpark")
        .config(conf=conf)
        .enableHiveSupport()
    )
    spark = configure_spark_with_delta_pip(builder)
    return spark.getOrCreate()
