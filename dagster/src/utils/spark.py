import subprocess
from uuid import uuid4

import pyarrow_hotfix  # noqa: F401, pylint: disable=unused-import
from dagster_pyspark import PySparkResource
from delta import configure_spark_with_delta_pip
from pyspark import SparkConf, sql
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col, concat_ws, count, sha2, udf

from dagster import OpExecutionContext, OutputContext
from src.settings import settings
from src.utils.logger import get_context_with_fallback_logger
from src.utils.schema import get_schema_columns


def _get_host_ip():
    completed_process = subprocess.run(
        ["hostname", "-i"], capture_output=True, check=False
    )
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
    "spark.sql.warehouse.dir": settings.SPARK_WAREHOUSE_DIR,
    "spark.sql.catalogImplementation": "hive",
    "hive.metastore.uris": settings.HIVE_METASTORE_URI,
    "spark.driver.cores": settings.SPARK_DRIVER_CORES,
    "spark.driver.memory": settings.SPARK_DRIVER_MEMORY,
    "spark.executor.cores": settings.SPARK_DRIVER_CORES,
    "spark.executor.memory": settings.SPARK_DRIVER_MEMORY,
    "spark.authenticate": "true",
    "spark.authenticate.secret": settings.SPARK_RPC_AUTHENTICATION_SECRET,
    "spark.authenticate.enableSaslEncryption": "true",
    "spark.databricks.delta.properties.defaults.enableChangeDataFeed": "true",
    "spark.databricks.delta.properties.defaults.appendOnly": "false",
    "spark.databricks.delta.schema.autoMerge.enabled": "false",
    "spark.databricks.delta.catalog.update.enabled": "true",
    f"fs.azure.sas.{settings.AZURE_BLOB_CONTAINER_NAME}.{settings.AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net": (
        settings.AZURE_SAS_TOKEN
    ),
}

if settings.IN_PRODUCTION:
    spark_common_config.update(
        {
            "spark.driver.host": _get_host_ip(),
            "spark.driver.port": "4040",
        }
    )

spark_app_name = (
    f"giga-dagster{f'@{settings.COMMIT_SHA}' if settings.COMMIT_SHA else ''}"
)

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


def count_nulls_for_column(df, column_name):
    return df.select(
        count(col(column_name).isNull()).alias(f"{column_name}_null_count")
    ).first()[0]


def transform_columns(
    df: sql.DataFrame,
    columns: list[str],
    target_type: types.DataType,
    context: OutputContext = None,
) -> sql.DataFrame:
    logger = get_context_with_fallback_logger(context)

    for col_name in columns:
        if col_name not in df.columns:
            continue

        nulls_before = count_nulls_for_column(df, col_name)
        df = df.withColumn(col_name, col(col_name).cast(target_type))
        logger.info(f">> TRANSFORMED {target_type} for column {col_name}")
        nulls_after = count_nulls_for_column(df, col_name)

        if nulls_before != nulls_after:
            raise ValueError(
                f"Error: NULL count mismatch for column {col_name} after the cast."
            )

    return df


def transform_school_types(
    df: sql.DataFrame, context: OpExecutionContext | OutputContext = None
) -> sql.DataFrame:
    columns_convert_to_double = [
        "latitude",
        "longitude",
        "download_speed_govt",
        "download_speed_govt1",
        "download_speed_govt5",
        "download_speed_contracted",
        "fiber_node_distance",
        "microwave_node_distance",
        "nearest_LTE_distance",
        "nearest_UMTS_distance",
        "nearest_GSM_distance",
        "nearest_school_distance",
    ]
    columns_convert_to_int = [
        "school_establishment_year",
        "connectivity_govt_collection_year",
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
        "schools_within_10km",
    ]
    columns_convert_to_long = [
        "pop_within_1km",
        "pop_within_2km",
        "pop_within_3km",
        "pop_within_10km",
    ]
    columns_convert_to_string = [
        "school_id_giga",
        "school_id_gov",
        "school_name",
        "education_level",
        "education_level_govt",
        "connectivity_govt",
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
        "school_id_gov_type",
        "school_address",
        "is_school_open",
        "cellular_coverage_availability",
        "cellular_coverage_type",
        "nearest_LTE_id",
        "nearest_UMTS_id",
        "nearest_GSM_id",
    ]
    columns_convert_to_timestamp = [
        "connectivity_govt_ingestion_timestamp",
        "school_location_ingestion_timestamp",
    ]

    df = transform_columns(df, columns_convert_to_double, types.DoubleType(), context)
    df = transform_columns(df, columns_convert_to_int, types.IntegerType(), context)
    df = transform_columns(df, columns_convert_to_long, types.LongType(), context)
    df = transform_columns(df, columns_convert_to_string, types.StringType(), context)
    df = transform_columns(
        df, columns_convert_to_timestamp, types.TimestampType(), context
    )

    df.printSchema()
    return df


def transform_types(
    df: sql.DataFrame,
    schema_name: str,
    context: OpExecutionContext | OutputContext = None,
) -> sql.DataFrame:
    logger = get_context_with_fallback_logger(context)

    columns = get_schema_columns(df.sparkSession, schema_name)
    if schema_name == "qos":
        columns = [c for c in columns if c.name in df.columns]

    df = df.withColumns(
        {
            column.name: col(column.name).cast(column.dataType)
            for column in columns
            if column.name != "signature"
        }
    )
    logger.info("Transformed column types")
    df.printSchema()
    return df


def transform_qos_bra_types(
    df: sql.DataFrame, context: OutputContext = None
) -> sql.DataFrame:
    log_func = print if context is None else context.log.info

    columns_convert_to_int = [
        "ip_family",
    ]

    columns_convert_to_float = [
        "speed_upload",
        "speed_download",
        "roundtrip_time",
        "jitter_upload",
        "jitter_download",
        "rtt_packet_loss_pct",
        "latency",
    ]

    columns_convert_to_timestamp = [
        "timestamp",
    ]

    for col_name in columns_convert_to_int:
        df = df.withColumn(col_name, col(col_name).cast(types.IntegerType()))
        log_func(">> TRANSFORMED INT")

    for col_name in columns_convert_to_float:
        df = df.withColumn(col_name, col(col_name).cast(types.FloatType()))
        log_func(">> TRANSFORMED FLOAT")

    for col_name in columns_convert_to_timestamp:
        df = df.withColumn(col_name, col(col_name).cast(types.TimestampType()))
        log_func(">> TRANSFORMED TIMESTAMP")

    @udf(returnType=types.StringType())
    def generate_uuid():
        return str(uuid4())

    df = df.withColumn("id", generate_uuid())
    df = df.withColumn("date", col("timestamp").cast(types.DateType()))

    df.printSchema()
    return df


def compute_row_hash(df: sql.DataFrame):
    return df.withColumn("signature", sha2(concat_ws("|", *sorted(df.columns)), 256))
