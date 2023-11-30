import pandas as pd
from pyspark import sql

from dagster import OpExecutionContext, Output, asset
from src._utils.adls import get_output_filepath
from src.resources.datahub_emitter import create_domains, emit_metadata_to_datahub


@asset(
    io_manager_key="adls_io_manager",
    required_resource_keys={"adls_file_client", "pyspark"},
)
def raw(context: OpExecutionContext) -> pd.DataFrame:
    df = context.resources.adls_file_client.download_adls_csv_to_pandas(
        context.run_tags["dagster/run_key"], context.resources.pyspark.spark_session
    )
    context.log.info("CREATING DOMAINS IN DATAHUB")
    create_domains()
    emit_metadata_to_datahub(context, df=df)
    yield Output(df, metadata={"filepath": context.run_tags["dagster/run_key"]})


@asset(
    io_manager_key="adls_io_manager",
)
def bronze(context: OpExecutionContext, raw: sql.DataFrame) -> sql.DataFrame:
    emit_metadata_to_datahub(context, df=raw)
    yield Output(raw, metadata={"filepath": get_output_filepath(context)})


@asset(
    io_manager_key="adls_io_manager",
    required_resource_keys={"ge_data_context"},
    op_tags={"kind": "ge"},
)
def data_quality_results(context, bronze: sql.DataFrame):
    validations = [
        {
            "batch_request": {
                "datasource_name": "spark_datasource",
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
    emit_metadata_to_datahub(context, df_failed)
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
    df = context.resources.adls_file_client.download_adls_csv_to_spark_dataframe(
        context.run_tags["dagster/run_key"], context.resources.pyspark.spark_session
    )
    emit_metadata_to_datahub(context, df)
    yield Output(df, metadata={"filepath": get_output_filepath(context)})


@asset(
    io_manager_key="adls_io_manager",
    required_resource_keys={"adls_file_client"},
)
def manual_review_failed_rows(context: OpExecutionContext) -> sql.DataFrame:
    df = context.resources.adls_file_client.download_from_adls(
        context.run_tags["dagster/run_key"]
    )
    context.log.info(f"data={df}")
    emit_metadata_to_datahub(context, df)
    yield Output(df, metadata={"filepath": get_output_filepath(context)})


@asset(
    io_manager_key="adls_io_manager",
)
def silver(
    context: OpExecutionContext, manual_review_passed_rows: sql.DataFrame
) -> sql.DataFrame:
    emit_metadata_to_datahub(context, df=manual_review_passed_rows)
    yield Output(
        manual_review_passed_rows, metadata={"filepath": get_output_filepath(context)}
    )


@asset(
    io_manager_key="adls_io_manager",
)
def gold(context: OpExecutionContext, silver: sql.DataFrame) -> sql.DataFrame:
    emit_metadata_to_datahub(context, df=silver)
    yield Output(silver, metadata={"filepath": get_output_filepath(context)})


@asset(
    io_manager_key="adls_io_manager",
    required_resource_keys={"adls_file_client", "pyspark"},
)
def gold_delta_table_from_csv(context: OpExecutionContext) -> sql.DataFrame:
    df = context.resources.adls_file_client.download_adls_csv_to_spark_dataframe(
        context.run_tags["dagster/run_key"], context.resources.pyspark.spark_session
    )
    yield Output(df, metadata={"filepath": get_output_filepath(context)})
