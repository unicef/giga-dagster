from dagster_ge import ge_validation_op_factory

from dagster import AssetKey, AssetsDefinition, asset

expectation_suite_op = ge_validation_op_factory(
    name="bank_loan_expectations",
    datasource_name="raw__bank_loans",
    suite_name="bank_loan",
    validation_operator_name="action_list_operator",
)

test_expectation_suite_asset = AssetsDefinition.from_op(
    expectation_suite_op,
    keys_by_input_name={"dataset": AssetKey("raw__bank_loans")},
)


@asset(
    description="Do a full greatexpectations' data docs rebuild",
    non_argument_deps={AssetKey("raw__bank_loans")},
    required_resource_keys={"ge_data_context"},
    op_tags={"kind": "ge"},
)
def ge_data_docs(context):
    context.resources.ge_data_context.build_data_docs()
