import pandas as pd

from dagster import OpExecutionContext, Output, asset

# from dagster_ge import ge_validation_op_factory


@asset(io_manager_key="adls_io_manager", required_resource_keys={"adls_loader"})
def raw(context: OpExecutionContext) -> pd.DataFrame:
    context.log.info(f'context run tags: {context.run_tags["dagster/run_key"]}')

    df = context.resources.adls_loader.load_from_adls(
        context.run_tags["dagster/run_key"]
    )
    context.log.info(f"data={df}")

    yield Output(df, metadata={"filepath": context.run_tags["dagster/run_key"]})
    # return df  # io manager should upload this to raw bucket as csv


@asset(
    io_manager_key="adls_io_manager",
)
def bronze(context: OpExecutionContext, raw: pd.DataFrame) -> pd.DataFrame:
    df = raw
    df["d"] = 12345

    yield Output(df, metadata={"filepath": context.run_tags["dagster/run_key"]})
    # return Output(df, metadata={"filename": df.shape[0]})
    # return (
    #     raw_file.transform()
    # )  # io manager should upload this to bronze/<dataset_name> as deltatable


# expectation_suite_op = ge_validation_op_factory(
#     name="raw_file_expectations",
#     datasource_name="raw_file",
#     suite_name="data-quality-checks",
#     validation_operator_name="action_list_operator",
# )

# expectation_suite_asset = AssetsDefinition.from_op(
#     expectation_suite_op,
#     keys_by_input_name={"dataset": AssetKey("bronze")},
# )  # not sure yet if we can return dfs from this. runs on dfs but uploads as deltatable


@asset(
    io_manager_key="adls_io_manager",
)
def dq_passed_rows(context: OpExecutionContext, bronze: pd.DataFrame) -> pd.DataFrame:
    df = bronze
    df["baby"] = "shark"
    return df
    # return (
    #     expectations_suite_asset.passed_rows()
    # )  # io manager should upload this to staging/pending-review/<dataset_name> as deltatable


@asset(
    io_manager_key="adls_io_manager",
)
def dq_failed_rows(context: OpExecutionContext, bronze: pd.DataFrame) -> pd.DataFrame:
    df = bronze
    df["doodoo"] = "doodoo"
    return df
    # return (
    #     expectations_suite_asset.failed_rows()
    # )  # io manager should upload this to archive/gx_tests_failed as deltatable


@asset(
    io_manager_key="adls_io_manager",
)
def manual_review_passed_rows(
    context: OpExecutionContext, dq_passed_rows: pd.DataFrame
) -> pd.DataFrame:
    return dq_passed_rows  # io manager should upload this to staging/approved/<dataset_name> as deltatable


@asset(
    io_manager_key="adls_io_manager",
)
def manual_review_failed_rows(
    context: OpExecutionContext, dq_passed_rows: pd.DataFrame
) -> pd.DataFrame:
    return dq_passed_rows  # io manager should upload this to archive/manual_review_rejected as deltatable


########## unsure how manual review will go yet/how datasets will be moved to the correct folders so the assets are probably wrong


@asset(
    io_manager_key="adls_io_manager",
)
def silver(context: OpExecutionContext, manual_review_passed_rows) -> pd.DataFrame:
    df = manual_review_passed_rows
    df["e"] = False
    return df
    # return (
    #     manual_review_passed_rows.transform()
    # )  # io manager should upload this to silver/<dataset_name> as deltatable


@asset(
    io_manager_key="adls_io_manager",
)
def gold(context: OpExecutionContext, silver: pd.DataFrame) -> pd.DataFrame:
    df = silver
    df["f"] = True
    return df
    # return (
    #     silver.merge()
    # )  # io manager should upload this to gold/master_data as deltatable
