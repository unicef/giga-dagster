import pyarrow_hotfix  # noqa: F401
from dagster_pyspark import PySparkResource
from delta import configure_spark_with_delta_pip
from pyspark import SparkConf, sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from dagster import OutputContext
from src.settings import settings

spark_common_config = {
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
    "spark.driver.memory": "768m",
    "spark.executor.cores": "1",
    "spark.executor.memory": "768m",
    "spark.shuffle.service.enabled": "false",
    "spark.dynamicAllocation.enabled": "false",
    "spark.dynamicAllocation.maxExecutors": "3",
    "spark.dynamicAllocation.executorAllocationRatio": "0.333",
    "spark.authenticate": "true",
    "spark.authenticate.secret": settings.SPARK_RPC_AUTHENTICATION_SECRET,
    "spark.authenticate.enableSaslEncryption": "true",
    "spark.databricks.delta.properties.defaults.enableChangeDataFeed": "true",
    "spark.databricks.delta.properties.defaults.appendOnly": "false",
    "spark.databricks.delta.schema.autoMerge.enabled": "false",
    f"fs.azure.sas.{settings.AZURE_BLOB_CONTAINER_NAME}.{settings.AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net": (
        settings.AZURE_SAS_TOKEN
    ),
    # "spark.python.use.daemon": "true",
    # "spark.python.daemon.module": "src.utils.sentry",
}

pyspark = PySparkResource(
    spark_config={
        "spark.app.name": (
            f"giga-dagster{f'@{settings.SHORT_SHA}' if settings.SHORT_SHA else ''}"
        ),
        "spark.master": f"spark://{settings.SPARK_MASTER_HOST}",
        **spark_common_config,
    }
)


def get_spark_session() -> SparkSession:
    conf = SparkConf()
    for key, value in spark_common_config.items():
        conf.set(key, value)

    builder = (
        SparkSession.builder.master(f"spark://{settings.SPARK_MASTER_HOST}")
        .appName("GigaDagsterSpark")
        .config(conf=conf)
        .enableHiveSupport()
    )
    spark = configure_spark_with_delta_pip(builder)
    return spark.getOrCreate()


def transform_dataframe_for_deltatable(
    context: OutputContext, df: sql.DataFrame
) -> sql.DataFrame:
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
        df = df.withColumn(col_name, col(col_name).cast("string"))
        context.log.info(">> TRANSFORMED STRING")

    for col_name in columns_convert_to_double:
        df = df.withColumn(col_name, col(col_name).cast("double"))
        context.log.info(">> TRANSFORMED DOUBLE")

    for col_name in columns_convert_to_int:
        df = df.withColumn(col_name, col(col_name).cast("int"))
        context.log.info(">> TRANSFORMED INT")

    for col_name in columns_convert_to_long:
        df = df.withColumn(col_name, col(col_name).cast("long"))
        context.log.info(">> TRANSFORMED LONG")

    df.printSchema()
    return df


def transform_dataframe_for_deltatable_no_context(df: sql.DataFrame) -> sql.DataFrame:
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
        df = df.withColumn(col_name, col(col_name).cast("string"))
        print(">> TRANSFORMED STRING")

    for col_name in columns_convert_to_double:
        df = df.withColumn(col_name, col(col_name).cast("double"))
        print(">> TRANSFORMED DOUBLE")

    for col_name in columns_convert_to_int:
        df = df.withColumn(col_name, col(col_name).cast("int"))
        print(">> TRANSFORMED INT")

    for col_name in columns_convert_to_long:
        df = df.withColumn(col_name, col(col_name).cast("long"))
        print(">> TRANSFORMED LONG")

    df.printSchema()
    return df
