import datetime as dt

from dagster_pyspark import PySparkResource
from pyspark.sql import SparkSession
from src.custom.qos.bra.common import fetch_and_build_dataframes
from src.custom.qos.bra.constants import COUNTRY_CODE
from src.custom.qos.bra.schema import BRA_QOS_SCHEMA
from src.custom.qos.schema_utils import enforce_schema, to_parquet_bytes
from src.utils.adls import ADLSFileClient

from dagster import OpExecutionContext, Output, asset


@asset
def bra_qos(context: OpExecutionContext, spark: PySparkResource) -> Output:
    s: SparkSession = spark.spark_session
    run_timestamp = dt.datetime.utcnow()
    query_date = run_timestamp.strftime("%Y-%m-%d")
    stamp = run_timestamp.strftime("%Y-%m-%d_%H-%M-%S")

    raw_bytes, silver_df, gold_df, error_df = fetch_and_build_dataframes(
        query_date, s, context
    )

    ADLSFileClient.upload_raw(None, raw_bytes, f"bronze/qos/{COUNTRY_CODE}/{stamp}.json")

    silver_df = enforce_schema(silver_df, BRA_QOS_SCHEMA)
    ADLSFileClient.upload_raw(
        None,
        to_parquet_bytes(silver_df),
        f"silver/qos/{COUNTRY_CODE}/{stamp}.parquet",
    )

    gold_df = enforce_schema(gold_df, BRA_QOS_SCHEMA)
    gold_parquet = to_parquet_bytes(gold_df)
    gold_filepath = f"gold/qos/{COUNTRY_CODE}/{stamp}.parquet"
    ADLSFileClient.upload_raw(None, gold_parquet, gold_filepath)

    if len(error_df) > 0:
        ADLSFileClient.upload_raw(
            None,
            to_parquet_bytes(error_df),
            f"silver/qos/{COUNTRY_CODE}/error-table/{stamp}.parquet",
        )

    context.log.info(f"{len(gold_df)} gold rows -> {gold_filepath}")
    return Output(
        None,
        metadata={"rows": len(gold_df), "error_rows": len(error_df), "filepath": gold_filepath},
    )
