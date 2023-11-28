from pyspark import sql
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType

from dagster import OpExecutionContext, Output, asset  # AssetsDefinition
from src._utils.adls import get_output_filepath

# from src.resources.datahub_emitter import create_domains, emit_metadata_to_datahub

# from dagster_ge import ge_validation_op_factory


@asset(
    io_manager_key="adls_io_manager",
    required_resource_keys={"adls_file_client", "pyspark"},
)
def raw(context: OpExecutionContext) -> sql.DataFrame:
    # Load data
    df = context.resources.adls_file_client.download_adls_csv_to_spark_dataframe(
        context.run_tags["dagster/run_key"], context.resources.pyspark.spark_session
    )
    context.log.info(df.head())

    # Create domains in Datahub
    # Emit metadata! This is a blocking call
    context.log.info("CREATING DOMAINS IN DATAHUB")
    # create_domains()

    # Emit metadata of dataset to Datahub
    # emit_metadata_to_datahub(context, df=df)

    # Yield output
    yield Output(df, metadata={"filepath": context.run_tags["dagster/run_key"]})


@asset(
    io_manager_key="adls_io_manager",
)
def bronze(context: OpExecutionContext, raw: sql.DataFrame) -> sql.DataFrame:
    # Run bronze layer transforms, standardize columns

    # Emit metadata of dataset to Datahub
    # emit_metadata_to_datahub(context, df=raw)

    # Yield output
    yield Output(raw, metadata={"filepath": get_output_filepath(context)})


@asset(
    io_manager_key="adls_io_manager",
    required_resource_keys={"ge_data_context"},
    op_tags={"kind": "ge"},
)
def data_quality_results(context, bronze: sql.DataFrame):
    # Run data quality checks
    validations = [
        {
            "batch_request": {
                "datasource_name": "pandas_datasource",
                "runtime_parameters": {"batch_data": bronze},
                "data_connector_name": "runtime_data_connector",
                "data_asset_name": "bronze_school_data",
                "batch_identifiers": {
                    "name": get_output_filepath(context),
                    "step": "bronze",
                },
            },
            "expectation_suite_name": "expectation_school_geolocation",
        },
    ]
    dq_results = context.resources.ge_data_context.run_checkpoint(
        checkpoint_name="school_geolocation_checkpoint", validations=validations
    )

    # Yield output
    yield Output(
        dq_results.to_json_dict(), metadata={"filepath": get_output_filepath(context)}
    )


@asset(
    io_manager_key="adls_io_manager",
)
def dq_passed_rows(
    context: OpExecutionContext, bronze: sql.DataFrame, data_quality_results
) -> sql.DataFrame:
    # Parse results, add column 'has_critical_error' to dataframe. Refer to this for dealing with results: https://docs.greatexpectations.io/docs/reference/api/checkpoint/types/checkpoint_result/checkpointresult_class/
    failed_rows_indices = set()
    # for suite_result in data_quality_results["run_results"].items():
    #     context.log.info(f"suite_result={suite_result}, {type(suite_result)}")
    #     validation_result = suite_result["validation_result"]
    #     for result in validation_result.results:
    #         if not result.success:
    #             for unexpected_row in result.result.unexpected_index_list:
    #                 failed_rows_indices.add(unexpected_row)

    df_passed = bronze.drop(index=list(failed_rows_indices))

    # Yield output
    yield Output(df_passed, metadata={"filepath": get_output_filepath(context)})


@asset(
    io_manager_key="adls_io_manager",
)
def dq_failed_rows(
    context: OpExecutionContext, bronze: sql.DataFrame, data_quality_results
) -> sql.DataFrame:
    # Parse results, add column 'has_critical_error' to dataframe. Refer to this for dealing with results: https://docs.greatexpectations.io/docs/reference/api/checkpoint/types/checkpoint_result/checkpointresult_class/
    failed_rows_indices = set()
    # for suite_result in data_quality_results["run_results"].items():
    #     validation_result = suite_result["validation_result"]
    #     for result in validation_result.results:
    #         if not result.success:
    #             for unexpected_row in result.result.unexpected_index_list:
    #                 failed_rows_indices.add(unexpected_row)

    df_failed = bronze.loc[list(failed_rows_indices)]

    # Emit metadata of dataset to Datahub
    # emit_metadata_to_datahub(context)

    # Yield output
    yield Output(df_failed, metadata={"filepath": get_output_filepath(context)})


# Would want to refactor the above code to a multi-asset, but it doesn't work yet. Have asked in Dagster slack, still waiting for response
# @multi_asset(
#     deps={AssetKey("ge_data_docs")},
#     outs={
#         "dq_passed_rows": AssetOut(
#             is_required=False, io_manager_key="adls_io_manager"
#         ),
#         "dq_failed_rows": AssetOut(
#             is_required=False, io_manager_key="adls_io_manager"
#         ),
#     },
# )
# def dq_split_rows(context: OpExecutionContext, data_quality_results, bronze: DataFrame) -> DataFrame:
#     failed_rows_indices = set()
#     for suite_result in data_quality_results["run_results"].values():
#         for result in suite_result["validation_result"]["results"]:
#             if not result["success"]:
#                 for unexpected_row in result["result"]["unexpected_index_list"]:
#                     failed_rows_indices.add(unexpected_row)

#     df = bronze
#     df_passed = df.drop(index=list(failed_rows_indices))
#     df_failed = df.loc[list(failed_rows_indices)]

#     yield Output(df_passed, output_name="dq_passed_rows", metadata={"filepath": get_output_filepath(context)})
#     yield Output(df_failed, output_name="dq_failed_rows", metadata={"filepath": get_output_filepath(context)})


@asset(
    io_manager_key="adls_io_manager",
    required_resource_keys={"adls_file_client", "pyspark"},
)
def manual_review_passed_rows(context: OpExecutionContext) -> sql.DataFrame:
    # Load data
    df = context.resources.adls_file_client.download_adls_csv_to_spark_dataframe(
        context.run_tags["dagster/run_key"], context.resources.pyspark.spark_session
    )
    context.log.info(f"data={df}")

    # Emit metadata of dataset to Datahub
    # emit_metadata_to_datahub(context, df)

    # Yield output
    yield Output(df, metadata={"filepath": get_output_filepath(context)})


@asset(
    io_manager_key="adls_io_manager",
    required_resource_keys={"adls_file_client"},
)
def manual_review_failed_rows(context: OpExecutionContext) -> sql.DataFrame:
    # Load data
    df = context.resources.adls_file_client.download_from_adls(
        context.run_tags["dagster/run_key"]
    )
    context.log.info(f"data={df}")

    # Emit metadata of dataset to Datahub
    # emit_metadata_to_datahub(context, df)

    # Yield output
    yield Output(df, metadata={"filepath": get_output_filepath(context)})


@asset(
    io_manager_key="adls_io_manager",
)
def silver(
    context: OpExecutionContext, manual_review_passed_rows: sql.DataFrame
) -> sql.DataFrame:
    # Run silver layer transforms

    # Emit metadata of dataset to Datahub
    # emit_metadata_to_datahub(context, df=manual_review_passed_rows)

    # Yield output
    yield Output(
        manual_review_passed_rows, metadata={"filepath": get_output_filepath(context)}
    )


@asset(
    io_manager_key="adls_io_manager",
)
def gold(context: OpExecutionContext, silver: sql.DataFrame) -> sql.DataFrame:
    # Run gold layer transforms - merge data

    # Emit metadata of dataset to Datahub
    # emit_metadata_to_datahub(context, df=silver)

    # Yield output
    yield Output(silver, metadata={"filepath": get_output_filepath(context)})


@asset(
    io_manager_key="adls_io_manager",
    required_resource_keys={"adls_file_client", "pyspark"},
)
def fake_gold(context: OpExecutionContext) -> sql.DataFrame:
    # Load data
    df: sql.DataFrame = (
        context.resources.adls_file_client.download_adls_csv_to_spark_dataframe(
            context.run_tags["dagster/run_key"], context.resources.pyspark.spark_session
        )
    )

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
            df = df.withColumn(col_name, col(col_name).cast(StringType()))
            context.log.info(">> TRANSFORMED STRING")
        except Exception as e:
            df = df.withColumn(col_name, col(col_name).cast(StringType()))
            context.log.warn(f"Failed to cast '{col_name}' to String: {str(e)}")

    for col_name in columns_convert_to_double:
        try:
            df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
            context.log.info(">> TRANSFORMED DOUBLE")
        except Exception as e:
            df = df.withColumn(col_name, col(col_name).cast(StringType()))
            context.log.warn(f"Failed to cast '{col_name}' to Double: {str(e)}")

    for col_name in columns_convert_to_int:
        try:
            df = df.withColumn(col_name, col(col_name).cast(IntegerType()))
            context.log.info(">> TRANSFORMED INT")
        except Exception as e:
            df = df.withColumn(col_name, col(col_name).cast(StringType()))
            context.log.warn(f"Failed to cast '{col_name}' to Int: {str(e)}")

    for col_name in columns_convert_to_long:
        try:
            df = df.withColumn(col_name, col(col_name).cast(LongType()))
            context.log.info(">> TRANSFORMED LONG")
        except Exception as e:
            df = df.withColumn(col_name, col(col_name).cast(StringType()))
            context.log.warn(f"Failed to cast '{col_name}' to Long: {str(e)}")

    df.show()
    df.printSchema()
    # for col in columns_convert_to_float:
    #     df[col] = df[col].astype(Float32Dtype())
    # for col in columns_convert_to_double:
    #     df[col] = df[col].astype(Float64Dtype())
    # for col in columns_convert_to_int:
    #     df[col] = df[col].astype(Float32Dtype()).astype(Int32Dtype())
    # for col in columns_convert_to_long:
    #     df[col] = df[col].astype(Float32Dtype()).astype(Int64Dtype())

    # Emit metadata of dataset to Datahub
    # emit_metadata_to_datahub(context, df)

    # Yield output
    yield Output(df, metadata={"filepath": get_output_filepath(context)})
