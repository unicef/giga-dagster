import pyarrow_hotfix  # noqa: F401
from dagster_pyspark import PySparkResource
from pyspark import sql

from dagster import OutputContext
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


def transform_dataframe_for_deltatable(context: OutputContext, df: sql.DataFrame):
    columns_convert_to_string = [
        "giga_id_school",
        "school_id",
        "name",
        "education_level",
        "education_level_regional",
        "school_type",
        "connectivity",
        "type_connectivity",
        "coverage_availability",
        "coverage_type",
        "admin1",
        "admin2",
        "admin3",
        "admin4",
        "school_region",
        "computer_availability",
        "computer_lab",
        "electricity",
        "water",
        "address",
    ]

    columns_convert_to_double = [
        "lat",
        "lon",
        "connectivity_speed",
        "latency_connectivity",
        "fiber_node_distance",
        "microwave_node_distance",
        "nearest_school_distance",
        "nearest_LTE_distance",
        "nearest_UMTS_distance",
        "nearest_GSM_distance",
    ]

    columns_convert_to_int = [
        "num_computers",
        "num_teachers",
        "num_students",
        "num_classroom",
        "nearest_LTE_id",
        "nearest_UMTS_id",
        "nearest_GSM_id",
        "schools_within_1km",
        "schools_within_2km",
        "schools_within_3km",
        "schools_within_10km",
    ]
    columns_convert_to_long = [
        "pop_within_1km",
        "pop_within_2km",
        "pop_within_3km",
        "pop_within_10km",
    ]

    for col_name in columns_convert_to_string:
        try:
            df = df.withColumn(
                col_name, sql.functions.col(col_name).cast(sql.types.StringType())
            )
            context.log.info(">> TRANSFORMED STRING")
        except Exception as e:
            df = df.withColumn(
                col_name, sql.functions.col(col_name).cast(sql.types.StringType())
            )
            context.log.warn(f"Failed to cast '{col_name}' to String: {str(e)}")

    for col_name in columns_convert_to_double:
        try:
            df = df.withColumn(
                col_name, sql.functions.col(col_name).cast(sql.types.DoubleType())
            )
            context.log.info(">> TRANSFORMED DOUBLE")
        except Exception as e:
            df = df.withColumn(
                col_name, sql.functions.col(col_name).cast(sql.types.StringType())
            )
            context.log.warn(f"Failed to cast '{col_name}' to Double: {str(e)}")

    for col_name in columns_convert_to_int:
        try:
            df = df.withColumn(
                col_name, sql.functions.col(col_name).cast(sql.types.IntegerType())
            )
            context.log.info(">> TRANSFORMED INT")
        except Exception as e:
            df = df.withColumn(
                col_name, sql.functions.col(col_name).cast(sql.types.StringType())
            )
            context.log.warn(f"Failed to cast '{col_name}' to Int: {str(e)}")

    for col_name in columns_convert_to_long:
        try:
            df = df.withColumn(
                col_name, sql.functions.col(col_name).cast(sql.types.LongType())
            )
            context.log.info(">> TRANSFORMED LONG")
        except Exception as e:
            df = df.withColumn(
                col_name, sql.functions.col(col_name).cast(sql.types.StringType())
            )
            context.log.warn(f"Failed to cast '{col_name}' to Long: {str(e)}")

    df.show()
    df.printSchema()
