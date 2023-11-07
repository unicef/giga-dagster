import pandas as pd
from dagster_ge import ge_validation_op_factory

from dagster import AssetKey, AssetsDefinition, OpExecutionContext, asset


@asset(required_resource_keys={"asset_loader"}, io_manager_key="adls_io_manager")
def raw_file(context: OpExecutionContext) -> pd.DataFrame:
    df = context.resources.asset_loader.load_file()
    return df  # io manager should upload this to raw bucket as csv


@asset(
    io_manager_key="adls_io_manager",
)
def bronze(context: OpExecutionContext, raw_file) -> pd.DataFrame:
    return (
        raw_file.transform()
    )  # io manager should upload this to bronze/<dataset_name> as deltatable


expectation_suite_op = ge_validation_op_factory(
    name="raw_file_expectations",
    datasource_name="raw_file",
    suite_name="data-quality-checks",
    validation_operator_name="action_list_operator",
)

expectation_suite_asset = AssetsDefinition.from_op(
    expectation_suite_op,
    keys_by_input_name={"dataset": AssetKey("bronze")},
)  # not sure yet if we can return dfs from this. runs on dfs but uploads as deltatable


@asset(
    io_manager_key="adls_io_manager",
)
def dq_passed_rows(
    context: OpExecutionContext, expectations_suite_asset
) -> pd.DataFrame:
    return (
        expectations_suite_asset.passed_rows()
    )  # io manager should upload this to staging/pending-review/<dataset_name> as deltatable


@asset(
    io_manager_key="adls_io_manager",
)
def dq_failed_rows(
    context: OpExecutionContext, expectations_suite_asset
) -> pd.DataFrame:
    return (
        expectations_suite_asset.failed_rows()
    )  # io manager should upload this to archive/gx_tests_failed as deltatable


@asset(
    io_manager_key="adls_io_manager",
)
def manual_review_passed_rows(
    context: OpExecutionContext, dq_passed_rows
) -> pd.DataFrame:
    return dq_passed_rows  # io manager should upload this to staging/approved/<dataset_name> as deltatable


@asset(
    io_manager_key="adls_io_manager",
)
def manual_review_failed_rows(
    context: OpExecutionContext, dq_passed_rows
) -> pd.DataFrame:
    return dq_passed_rows  # io manager should upload this to archive/manual_review_rejected as deltatable


########## unsure how manual review will go yet/how datasets will be moved to the correct folders so the assets are probably wrong


@asset(
    io_manager_key="adls_io_manager",
)
def silver(context: OpExecutionContext, manual_review_passed_rows) -> pd.DataFrame:
    return (
        manual_review_passed_rows.transform()
    )  # io manager should upload this to silver/<dataset_name> as deltatable


@asset(
    io_manager_key="adls_io_manager",
)
def gold(context: OpExecutionContext, silver) -> pd.DataFrame:
    return (
        silver.merge()
    )  # io manager should upload this to gold/master_data as deltatable
