from unittest.mock import MagicMock, patch

import pytest
from pyspark import sql
from pyspark.sql.types import IntegerType, StringType
from src.utils.datahub.emit_dataset_metadata import (
    create_dataset_urn,
    datahub_emit_metadata_with_exception_catcher,
    define_dataset_properties,
    emit_metadata_to_datahub,
)
from src.utils.op_config import FileConfig

from dagster import DagsterInstance, OpExecutionContext


@pytest.fixture
def mock_context():
    context = MagicMock(spec=OpExecutionContext)
    context.log = MagicMock()
    context.run_tags = {"dagster/sensor_name": "test_sensor"}
    context.instance = MagicMock(spec=DagsterInstance)
    context.instance.get_run_stats.return_value.start_time = 1672531200
    context.run_id = "test_run_id"
    context.run.is_resume_retry = False
    context.run.parent_run_id = None
    context.run.root_run_id = "root_run_id"
    context.op_def.name = "test_op"
    context.job_def.name = "test_job"
    context.asset_key.to_user_string.return_value = "test_asset"
    config_dict = {
        "filepath": "/path/to/file.csv",
        "dataset_type": "master",
        "file_size_bytes": 1024,
        "destination_filepath": "/path/to/start/file.csv",
        "country_code": "BRA",
        "tier": "raw",
        "metastore_schema": "school_master",
        "domain": "school",
    }
    step_context = MagicMock()
    step_context.op_config = config_dict
    context.get_step_execution_context.return_value = step_context
    return context


@patch("src.utils.datahub.emit_dataset_metadata.builder")
def test_create_dataset_urn(mock_builder, mock_context):
    mock_builder.make_data_platform_urn.return_value = "urn:li:dataPlatform:adlsGen2"
    mock_builder.make_dataset_urn.return_value = "urn:li:dataset:test"

    base_urn = create_dataset_urn(mock_context, is_upstream=False)
    assert base_urn == "urn:li:dataset:test"
    mock_builder.make_dataset_urn.assert_called()


@patch("src.utils.datahub.emit_dataset_metadata.identify_country_name")
def test_define_dataset_properties(mock_identify_country, mock_context):
    mock_identify_country.return_value = "Brazil"

    props = define_dataset_properties(mock_context, country_code="BRA")
    assert props.customProperties["Country"] == "Brazil"
    assert props.customProperties["Data Format"] == "csv"


@patch("src.utils.datahub.emit_dataset_metadata.datahub_emitter")
@patch("src.utils.datahub.emit_dataset_metadata.datahub_graph_client")
@patch("src.utils.datahub.emit_dataset_metadata.identify_country_name")
def test_emit_metadata_to_datahub(
    mock_identify_country, mock_graph, mock_emitter, mock_context
):
    mock_identify_country.return_value = "Brazil"
    dataset_urn = "urn:li:dataset:test"

    schema_ref = [("col1", "string"), ("col2", "int")]

    emit_metadata_to_datahub(
        context=mock_context,
        country_code="BRA",
        dataset_urn=dataset_urn,
        schema_reference=schema_ref,
    )

    assert mock_emitter.emit.call_count >= 2
    assert mock_graph.execute_graphql.call_count >= 2


@patch("src.utils.datahub.emit_dataset_metadata.define_schema_properties")
@patch("src.utils.datahub.emit_dataset_metadata.datahub_emitter")
@patch("src.utils.datahub.emit_dataset_metadata.datahub_graph_client")
@patch("src.utils.datahub.emit_dataset_metadata.identify_country_name")
def test_emit_metadata_spark_schema(
    mock_identify, mock_graph, mock_emitter, _, mock_context
):
    mock_identify.return_value = "Brazil"

    mock_df = MagicMock(spec=sql.DataFrame)
    mock_field1 = MagicMock()
    mock_field1.name = "col1"
    mock_field1.dataType = StringType()
    mock_field2 = MagicMock()
    mock_field2.name = "col2"
    mock_field2.dataType = IntegerType()

    mock_df.schema.fields = [mock_field1, mock_field2]

    emit_metadata_to_datahub(
        context=mock_context,
        country_code="BRA",
        dataset_urn="urn:li:dataset:test",
        schema_reference=mock_df,
    )

    assert mock_emitter.emit.call_count >= 2

    assert mock_graph.execute_graphql.call_count >= 2


@patch("src.utils.datahub.emit_dataset_metadata.update_policy_for_group")
@patch("src.utils.datahub.emit_dataset_metadata.add_column_metadata")
@patch("src.utils.datahub.emit_dataset_metadata.get_column_licenses")
@patch("src.utils.datahub.emit_dataset_metadata.get_schema_column_descriptions")
@patch("src.utils.datahub.emit_dataset_metadata.emit_lineage")
@patch("src.utils.datahub.emit_dataset_metadata.emit_metadata_to_datahub")
@patch("src.utils.datahub.emit_dataset_metadata.should_emit_metadata")
def test_datahub_emit_metadata_wrapper(
    mock_should,
    mock_emit,
    mock_lineage,
    mock_get_desc,
    mock_get_licenses,
    mock_add_col,
    mock_policy,
    mock_context,
):
    mock_should.return_value = True
    mock_get_licenses.return_value = "license"
    mock_get_desc.return_value = "description"

    config = FileConfig(
        filepath="/path.csv",
        dataset_type="master",
        destination_filepath="/dest.csv",
        file_size_bytes=100,
        country_code="BRA",
        metastore_schema="schema",
        tier="raw",
    )

    mock_spark = MagicMock()

    datahub_emit_metadata_with_exception_catcher(
        context=mock_context,
        config=config,
        spark=mock_spark,
        schema_reference=[("col", "string")],
    )

    mock_emit.assert_called()
    mock_lineage.assert_called()
    mock_add_col.assert_called()
    mock_policy.assert_called()

    mock_emit.side_effect = Exception("Emit Failed")
    datahub_emit_metadata_with_exception_catcher(context=mock_context, config=config)
    mock_context.log.error.assert_called()
