from io import BytesIO

import pandas as pd
import sentry_sdk
from dagster_pyspark import PySparkResource
from numpy import nan
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from src.resources import ResourceKey
from src.utils.adls import ADLSFileClient
from src.utils.datahub.emit_dataset_metadata import emit_metadata_to_datahub
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig
from src.utils.schema import get_schema_columns_datahub
from src.utils.sentry import log_op_context
from src.utils.spark import transform_types

from dagster import (
    OpExecutionContext,
    Output,
    asset,
)


@asset(io_manager_key=ResourceKey.ADLS_PASSTHROUGH_IO_MANAGER.value)
def adhoc__load_qos_csv(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    config: FileConfig,
) -> Output[bytes]:
    raw = adls_file_client.download_raw(config.filepath)
    emit_metadata_to_datahub(
        context,
        df=raw,
        country_code=config.filename_components.country_code,
        dataset_urn=config.datahub_source_dataset_urn,
    )
    return Output(raw, metadata=get_output_metadata(config))


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def adhoc__qos_transforms(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: FileConfig,
    adhoc__load_qos_csv: bytes,
) -> Output[pd.DataFrame]:
    s: SparkSession = spark.spark_session

    with BytesIO(adhoc__load_qos_csv) as buffer:
        buffer.seek(0)
        df = pd.read_csv(buffer).fillna(nan).replace([nan], [None])

    sdf = s.createDataFrame(df)
    column_actions = {
        "signature": f.sha2(f.concat_ws("|", *sdf.columns), 256),
        "gigasync_id": f.sha2(
            f.concat_ws(
                "_",
                f.col("school_id_giga"),
                f.col("timestamp"),
            ),
            256,
        ),
        "date": f.to_date(f.col("timestamp")),
    }
    sdf = sdf.withColumns(column_actions).dropDuplicates(["gigasync_id"])
    context.log.info(f"Calculated SHA256 signature for {sdf.count()} rows")

    emit_metadata_to_datahub(
        context,
        df=sdf,
        country_code=config.filename_components.country_code,
        dataset_urn=config.datahub_source_dataset_urn,
    )
    df_pandas = sdf.toPandas()
    return Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_DELTA_V2_IO_MANAGER.value)
def adhoc__publish_qos_to_gold(
    context: OpExecutionContext,
    adhoc__qos_transforms: sql.DataFrame,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[sql.DataFrame]:
    df_transformed = transform_types(
        adhoc__qos_transforms, config.metastore_schema, context
    )

    schema_reference = get_schema_columns_datahub(
        spark.spark_session, config.metastore_schema
    )
    try:
        emit_metadata_to_datahub(
            context,
            df=schema_reference,
            country_code=config.filename_components.country_code,
            dataset_urn=config.datahub_source_dataset_urn,
        )
    except Exception as error:
        context.log.error(f"Error on Datahub Emit Metadata: {error}")
        log_op_context(context)
        sentry_sdk.capture_exception(error=error)

    return Output(
        df_transformed,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_transformed),
        },
    )
