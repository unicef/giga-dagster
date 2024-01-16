import subprocess

import pyarrow_hotfix  # noqa: F401
from dagster_pyspark import PySparkResource
from delta import configure_spark_with_delta_pip
from pyspark import SparkConf, sql
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col

from dagster import OutputContext
from src.settings import settings


def _get_host_ip():
    completed_process = subprocess.run(["hostname", "-i"], capture_output=True)
    ip = completed_process.stdout.strip().decode("utf-8")
    return "127.0.0.1" if ip == "127.0.1.1" else ip


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
    "spark.driver.memory": "1g",
    "spark.executor.cores": "1",
    "spark.executor.memory": "1g",
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

if settings.IN_PRODUCTION:
    spark_common_config.update(
        {
            "spark.driver.host": _get_host_ip(),
            "spark.driver.port": "4040",
        }
    )

spark_app_name = f"giga-dagster{f'@{settings.SHORT_SHA}' if settings.SHORT_SHA else ''}"

pyspark = PySparkResource(
    spark_config={
        "spark.app.name": spark_app_name,
        "spark.master": f"spark://{settings.SPARK_MASTER_HOST}:7077",
        **spark_common_config,
    }
)


def get_spark_session() -> SparkSession:
    conf = SparkConf()
    for key, value in spark_common_config.items():
        conf.set(key, value)

    builder = (
        SparkSession.builder.master(f"spark://{settings.SPARK_MASTER_HOST}:7077")
        .appName(spark_app_name)
        .config(conf=conf)
        .enableHiveSupport()
    )
    spark = configure_spark_with_delta_pip(builder)
    return spark.getOrCreate()


def transform_school_master_types(
    df: sql.DataFrame, context: OutputContext = None
) -> sql.DataFrame:
    log_func = print if context is None else context.log.info

    columns_convert_to_string = [
        "school_id_giga",
        "school_id_gov",
        "school_name",
        "education_level",
        "connectivity_type_govt",
        "admin1",
        "admin2",
        "school_area_type",
        "school_funding_type",
        "computer_lab",
        "electricity_availability",
        "electricity_type",
        "water_availability",
        "school_data_source",
        "school_data_collection_modality",
        "cellular_coverage_availability",
        "cellular_coverage_type",
        "connectivity_govt",
        "connectivity",
        "connectivity_RT",
        "connectivity_RT_datasource",
        "disputed_region",
        "nearest_NR_id",
        "connectivity_static",
    ]

    columns_convert_to_double = [
        "latitude",
        "longitude",
        "download_speed_contracted",
        "fiber_node_distance",
        "microwave_node_distance",
        "nearest_LTE_distance",
        "nearest_UMTS_distance",
        "nearest_GSM_distance",
        "nearest_NR_distance",
    ]

    columns_convert_to_int = [
        "school_establishment_year",
        "num_computers",
        "num_computers_desired",
        "num_teachers",
        "num_adm_personnel",
        "num_students",
        "num_classroom",
        "num_latrines",
        "school_data_collection_year",
        "schools_within_1km",
        "schools_within_2km",
        "schools_within_3km",
        "connectivity_govt_collection_year",
    ]
    columns_convert_to_long = [
        "pop_within_1km",
        "pop_within_2km",
        "pop_within_3km",
    ]
    columns_convert_to_timestamp = [
        "school_location_ingestion_timestamp",
        "connectivity_RT_ingestion_timestamp",
        "connectivity_govt_ingestion_timestamp",
    ]

    for col_name in columns_convert_to_string:
        df = df.withColumn(col_name, col(col_name).cast(types.StringType()))
        log_func(">> TRANSFORMED STRING")

    for col_name in columns_convert_to_double:
        df = df.withColumn(col_name, col(col_name).cast(types.DoubleType()))
        log_func(">> TRANSFORMED DOUBLE")

    for col_name in columns_convert_to_int:
        df = df.withColumn(col_name, col(col_name).cast(types.IntegerType()))
        log_func(">> TRANSFORMED INT")

    for col_name in columns_convert_to_long:
        df = df.withColumn(col_name, col(col_name).cast(types.LongType()))
        log_func(">> TRANSFORMED LONG")

    for col_name in columns_convert_to_timestamp:
        df = df.withColumn(col_name, col(col_name).cast(types.TimestampType()))
        log_func(">> TRANSFORMED TIMESTAMP")

    df.printSchema()
    return df


def transform_school_reference_types(
    df: sql.DataFrame, context: OutputContext = None
) -> sql.DataFrame:
    log_func = print if context is None else context.log.info

    columns_convert_to_int = [
        "pop_within_10km",
        "schools_within_10km",
    ]
    columns_convert_to_float = [
        "download_speed_govt1",
        "download_speed_govt5",
        "nearest_school_distance",
    ]

    for col_name in columns_convert_to_int:
        df = df.withColumn(col_name, col(col_name).cast(types.IntegerType()))
        log_func(">> TRANSFORMED INT")

    for col_name in columns_convert_to_float:
        df = df.withColumn(col_name, col(col_name).cast(types.FloatType()))
        log_func(">> TRANSFORMED FLOAT")

    df.printSchema()
    return df
