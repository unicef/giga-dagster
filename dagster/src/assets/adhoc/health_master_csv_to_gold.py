from io import BytesIO

import pandas as pd
from dagster_pyspark import PySparkResource
import numpy as np
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import NullType
from src.resources import ResourceKey
from src.utils.adls import ADLSFileClient
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig
from src.utils.sentry import capture_op_exceptions
from src.utils.logger import ContextLoggerWithLoguruFallback
from src.spark.transform_functions import (
    add_missing_columns,
    create_health_id_giga,
    add_admin_columns
)
from src.utils.schema import (
    get_schema_columns,
)

from dagster import (
    OpExecutionContext,
    Output,
    asset,
)


@asset(io_manager_key=ResourceKey.ADLS_PASSTHROUGH_IO_MANAGER.value)
def adhoc__load_health_master_csv(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    config: FileConfig,
) -> Output[bytes]:
    raw = adls_file_client.download_raw(config.filepath)
    return Output(raw, metadata=get_output_metadata(config))


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
@capture_op_exceptions
def adhoc__health_master_data_transforms(
    context: OpExecutionContext,
    adhoc__load_health_master_csv: bytes,
    spark: PySparkResource,
    config: FileConfig,
) -> Output[pd.DataFrame]:

    logger = ContextLoggerWithLoguruFallback(context)
    s: SparkSession = spark.spark_session

    schema_columns = get_schema_columns(s, config.metastore_schema)
    with BytesIO(adhoc__load_health_master_csv) as buffer:
        buffer.seek(0)
        df = pd.read_csv(buffer).fillna(np.nan).replace([np.nan], [None])

    context.log.info(f"columns: {df.columns.tolist()}")
    context.log.info(f"row count: {len(df)}")

    df = df[[column.name for column in schema_columns if column.name in df.columns]]
    for column, dtype in df.dtypes.items():
        if dtype == "object":
            df[column] = df[column].astype("string")

    sdf = s.createDataFrame(df)

    sdf = add_missing_columns(sdf, schema_columns)
    sdf = create_health_id_giga(sdf)

    sdf = add_admin_columns(
        df=sdf,
        country_code_iso3=config.country_code,
        admin_level="admin1",
    )

    df = sdf.toPandas()
    df = df.drop_duplicates("health_id_giga")

    context.log.info(f"columns: {df.columns.tolist()}")
    context.log.info(f"row count: {len(df)}")

    return Output(
        df,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df)
        }
    )


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def adhoc__publish_health_master_to_gold(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: FileConfig,
    adhoc__health_master_data_transforms: sql.DataFrame,
) -> Output[sql.DataFrame]:
    s: SparkSession = spark.spark_session

    df = adhoc__health_master_data_transforms

    context.log.info("original schema")
    context.log.info(df.schema.simpleString())

    void_columns = [
        field.name for field in df.schema.fields if isinstance(field.dataType, NullType)
    ]

    for col in void_columns:
        context.log.info(f"{col}")
        df = df.withColumn(col, f.col(col).cast("string"))

    context.log.info("updated schema")
    context.log.info(df.schema.simpleString())

    return Output(
        df,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df),
        },
    )
