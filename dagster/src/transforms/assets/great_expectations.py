from dagster_ge import ge_validation_op_factory

from dagster import AssetKey, AssetsDefinition, asset

expectation_suite_op = ge_validation_op_factory(
    name="<your_asset_name>_expectations",
    datasource_name="raw__bank_loan",
    suite_name="<your_expectation_suite_name>",
    validation_operator_name="action_list_operator",
)

expectation_suite_asset = AssetsDefinition.from_op(
    expectation_suite_op,
    keys_by_input_name={"dataset": AssetKey("<your_asset_name>")},
)


@asset(
    description="Do a full greatexpectations' data docs rebuild",
    non_argument_deps={AssetKey("<your_asset_name>")},
    required_resource_keys={"ge_data_context"},
    op_tags={"kind": "ge"},
)
def ge_data_docs(context):
    context.resources.ge_data_context.build_data_docs()
