import pyarrow_hotfix  # noqa: F401
from dagster_pyspark import PySparkResource

from src.settings import settings

pyspark = PySparkResource(
    spark_config={
        "spark.app.name": f"giga-dagster@{settings.SHORT_SHA}",
        "spark.master": f"spark://{settings.SPARK_MASTER_HOST}",
        "spark.driver.extraJavaOptions": str.join(
            " ",
            [
                "-Divy.cache.dir=/tmp",
                "-Divy.home=/tmp",
                "-Dio.netty.tryReflectionSetAccessible=true",
            ],
        ),
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": (
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        ),
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.warehouse.dir": "/opt/spark/warehouse",
        "spark.sql.catalogImplementation": "hive",
        "spark.driver.cores": "1",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1",
        "spark.executor.memory": "512m",
        "spark.executor.instances": "1",
        "spark.authenticate": "true",
        "spark.authenticate.secret": settings.SPARK_RPC_AUTHENTICATION_SECRET,
        "spark.authenticate.enableSaslEncryption": "true",
        "spark.databricks.delta.properties.defaults.enableChangeDataFeed": "true",
        "spark.databricks.delta.properties.defaults.appendOnly": "false",
        f"fs.azure.sas.{settings.AZURE_BLOB_CONTAINER_NAME}.{settings.AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net": (
            settings.AZURE_SAS_TOKEN
        ),
    }
)
