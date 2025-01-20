from datetime import datetime

import pandas as pd
from dagster_pyspark import PySparkResource
from delta import DeltaTable
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.functions import udf
from src.constants import DataTier
from src.data_quality_checks.utils import (
    aggregate_report_json,
    aggregate_report_spark_df,
    dq_split_failed_rows,
    dq_split_passed_rows,
    row_level_checks,
)
from src.resources import ResourceKey
from src.utils.datahub.emit_dataset_metadata import (
    datahub_emit_metadata_with_exception_catcher,
)
from src.utils.db.primary import get_db_context
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig
from src.utils.qos_apis.school_connectivity import query_school_connectivity_data
from src.utils.schema import (
    construct_full_table_name,
    construct_schema_name_for_tier,
    get_schema_columns_datahub,
)
from src.utils.sentry import capture_op_exceptions

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key="adls_pandas_io_manager")
@capture_op_exceptions
def qos_school_connectivity_raw(
    context: OpExecutionContext,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[pd.DataFrame]:
    s: SparkSession = spark.spark_session
    database_data = config.row_data_dict
    context.log.info(f"Database row: {database_data}")

    complete_school_id_parameters = bool(
        database_data["school_id_key"]
        and database_data["school_list"]["school_id_key"]
        and database_data["school_id_send_query_in"] != "NONE"
    )

    df = pd.DataFrame()

    ## If the connectivity API requires school IDs to be passed as part of the request, get the list of unique school IDs from the silver geolocation table
    if complete_school_id_parameters:
        schema_name = "school_geolocation"
        silver_tier_schema_name = construct_schema_name_for_tier(
            schema_name, DataTier.SILVER
        )
        silver_table_name = construct_full_table_name(
            silver_tier_schema_name, config.country_code
        )

        if s.catalog.tableExists(silver_table_name):
            silver = DeltaTable.forName(s, silver_table_name).toDF()
            field_to_query = database_data["school_list"]["school_id_key"]
            silver_column_to_query = database_data["school_list"][
                "column_to_schema_mapping"
            ][field_to_query]

            unique_school_ids = (
                (silver.select(silver_column_to_query))
                .rdd.flatMap(lambda x: x)
                .collect()
            )

            with get_db_context() as database_session:
                for id in unique_school_ids:
                    try:
                        data = pd.DataFrame.from_records(
                            query_school_connectivity_data(
                                context, database_session, database_data, id
                            ),
                        )
                    except Exception as e:
                        context.log.info(e)
                        continue
                    else:
                        if not data.empty:
                            df = pd.concat([df, data], ignore_index=True)
        else:
            context.log.warning("Cannot query the API without a source of school IDs")
            return Output(
                df,
                metadata={
                    **get_output_metadata(config),
                    "preview": get_table_preview(df),
                },
            )

    else:
        with get_db_context() as database_session:
            df = pd.DataFrame.from_records(
                query_school_connectivity_data(
                    context, database_session, database_data
                ),
            )

    return Output(
        df,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
@capture_op_exceptions
def qos_school_connectivity_bronze(
    context: OpExecutionContext,
    qos_school_connectivity_raw: sql.DataFrame,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[pd.DataFrame]:
    s: SparkSession = spark.spark_session
    database_data = config.row_data_dict

    schema_name = "school_geolocation"
    silver_tier_schema_name = construct_schema_name_for_tier(
        schema_name, DataTier.SILVER
    )
    silver_table_name = construct_full_table_name(
        silver_tier_schema_name, config.country_code
    )

    if database_data["has_school_id_giga"]:
        qos_school_connectivity_raw = qos_school_connectivity_raw.withColumnRenamed(
            database_data["school_id_giga_govt_key"], "school_id_giga"
        )
    else:
        qos_school_connectivity_raw = qos_school_connectivity_raw.withColumnRenamed(
            database_data["school_id_giga_govt_key"], "school_id_govt"
        )
        qos_school_connectivity_raw = qos_school_connectivity_raw.withColumn(
            "school_id_giga", f.lit(None).cast("string")
        )

    bronze = s.createDataFrame(
        s.sparkContext.emptyRDD(), schema=qos_school_connectivity_raw.schema
    )

    # Filter out school connectivity data for schools not in the school geolocation silver table. Add school_id_giga to school_connectivity if it does not exist yet
    if s.catalog.tableExists(silver_table_name):
        silver = DeltaTable.forName(s, silver_table_name).toDF()

        if database_data["has_school_id_giga"]:
            bronze = qos_school_connectivity_raw.join(
                silver.select("school_id_giga"), on="school_id_giga", how="left_semi"
            )

        else:
            silver = silver.withColumnRenamed("school_id_giga", "silver_school_id_giga")
            bronze = qos_school_connectivity_raw.join(
                silver.select("school_id_govt", "silver_school_id_giga"),
                on="school_id_govt",
                how="inner",
            )
            bronze = bronze.withColumn(
                "school_id_giga", f.col("silver_school_id_giga")
            ).drop("silver_school_id_giga")

    @udf
    def parse_dates(value: str) -> str:
        return datetime.strptime(
            value, database_data["response_date_format"]
        ).isoformat()

    if not bronze.isEmpty():
        ## Transform the time partition column to the correct format
        if database_data["response_date_format"] == "ISO8601":
            # no transform needed actually, this is what we expect
            if database_data["response_date_key"] != "timestamp":
                bronze = bronze.withColumn(
                    "timestamp", f.col(database_data["response_date_key"])
                )
        elif database_data["response_date_format"] == "timestamp":
            bronze = bronze.withColumn(
                "timestamp",
                f.from_unixtime(f.col(database_data["response_date_key"])),
            )
        else:
            bronze = bronze.withColumn(
                "timestamp",
                parse_dates(f.col(database_data["response_date_key"])),
            )

        ## Add gigasync_id column
        column_actions = {
            "signature": f.sha2(f.concat_ws("|", *bronze.columns), 256),
            "gigasync_id": f.sha2(
                f.concat_ws(
                    "_",
                    f.col("school_id_giga"),
                    f.col("timestamp"),
                ),
                256,
            ),
            "date": f.to_date(f.col(database_data["response_date_key"])),
        }
        bronze = bronze.withColumns(column_actions).dropDuplicates(["gigasync_id"])
        context.log.info(f"Calculated SHA256 signature for {bronze.count()} rows")

    return Output(
        bronze.toPandas(),
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(bronze),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
@capture_op_exceptions
def qos_school_connectivity_data_quality_results(
    context: OpExecutionContext,
    config: FileConfig,
    qos_school_connectivity_bronze: sql.DataFrame,
) -> Output[pd.DataFrame]:
    country_code = config.country_code
    dq_results = row_level_checks(
        qos_school_connectivity_bronze,
        "qos",
        country_code,
        context=context,
    )

    dq_pandas = dq_results.toPandas()
    return Output(
        dq_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(dq_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_JSON_IO_MANAGER.value)
@capture_op_exceptions
def qos_school_connectivity_data_quality_results_summary(
    qos_school_connectivity_raw: sql.DataFrame,
    qos_school_connectivity_data_quality_results: sql.DataFrame,
    spark: PySparkResource,
    config: FileConfig,
) -> Output[dict]:
    dq_summary_statistics = aggregate_report_json(
        df_aggregated=aggregate_report_spark_df(
            spark.spark_session,
            qos_school_connectivity_data_quality_results,
        ),
        df_bronze=qos_school_connectivity_raw,
        df_data_quality_checks=qos_school_connectivity_data_quality_results,
    )

    return Output(dq_summary_statistics, metadata=get_output_metadata(config))


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
@capture_op_exceptions
def qos_school_connectivity_dq_passed_rows(
    qos_school_connectivity_data_quality_results: sql.DataFrame,
    config: FileConfig,
) -> Output[pd.DataFrame]:
    df_passed = dq_split_passed_rows(
        qos_school_connectivity_data_quality_results,
        "qos",
    )

    df_pandas = df_passed.toPandas()
    return Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
@capture_op_exceptions
def qos_school_connectivity_dq_failed_rows(
    qos_school_connectivity_data_quality_results: sql.DataFrame,
    config: FileConfig,
) -> Output[pd.DataFrame]:
    df_failed = dq_split_failed_rows(
        qos_school_connectivity_data_quality_results,
        "qos",
    )

    df_pandas = df_failed.toPandas()
    return Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
@capture_op_exceptions
def qos_school_connectivity_gold(
    context: OpExecutionContext,
    qos_school_connectivity_dq_passed_rows: sql.DataFrame,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[sql.DataFrame]:
    schema_reference = get_schema_columns_datahub(
        spark.spark_session, config.metastore_schema
    )
    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=schema_reference,
    )
    return Output(
        qos_school_connectivity_dq_passed_rows,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(qos_school_connectivity_dq_passed_rows),
        },
    )
