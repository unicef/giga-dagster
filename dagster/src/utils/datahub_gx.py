import datahub.emitter.mce_builder as builder
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.integrations.great_expectations.action import (
    DataHubValidationAction,
    convert_to_string,
    logger,
    make_dataset_urn_from_sqlalchemy_uri,
    warn,
)
from datahub.metadata.com.linkedin.pegasus2avro.assertion import BatchSpec
from datahub.metadata.schema_classes import PartitionSpecClass, PartitionTypeClass
from datahub.utilities.sql_parser import DefaultSQLParser
from great_expectations.core.batch_spec import (
    RuntimeQueryBatchSpec,
    SqlAlchemyDatasourceBatchSpec,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.validator.validator import Validator
from sqlalchemy.engine.base import Connection, Engine


class CustomDataHubValidationAction(DataHubValidationAction):
    def get_dataset_partitions(self, batch_identifier, data_asset):
        dataset_partitions = []

        logger.debug("Finding datasets being validated")

        # for now, we support only v3-api and sqlalchemy execution engine
        if isinstance(data_asset, Validator) and isinstance(
            data_asset.execution_engine, SqlAlchemyExecutionEngine
        ):
            ge_batch_spec = data_asset.active_batch_spec
            partitionSpec = None
            batchSpecProperties = {
                "data_asset_name": str(
                    data_asset.active_batch_definition.data_asset_name
                ),
                "datasource_name": str(
                    data_asset.active_batch_definition.datasource_name
                ),
            }
            sqlalchemy_uri = None
            if isinstance(data_asset.execution_engine.engine, Engine):
                sqlalchemy_uri = data_asset.execution_engine.engine.url
            # For snowflake sqlalchemy_execution_engine.engine is actually instance of Connection
            elif isinstance(data_asset.execution_engine.engine, Connection):
                sqlalchemy_uri = data_asset.execution_engine.engine.engine.url

            if isinstance(ge_batch_spec, SqlAlchemyDatasourceBatchSpec):
                # e.g. ConfiguredAssetSqlDataConnector with splitter_method or sampling_method
                schema_name = ge_batch_spec.get("schema_name")
                table_name = ge_batch_spec.get("table_name")

                dataset_urn = make_dataset_urn_from_sqlalchemy_uri(
                    sqlalchemy_uri,
                    schema_name,
                    table_name,
                    self.env,
                    self.get_platform_instance(
                        data_asset.active_batch_definition.datasource_name
                    ),
                    self.exclude_dbname,
                    self.platform_alias,
                    self.convert_urns_to_lowercase,
                )
                batchSpec = BatchSpec(
                    nativeBatchId=batch_identifier,
                    customProperties=batchSpecProperties,
                )

                splitter_method = ge_batch_spec.get("splitter_method")
                if (
                    splitter_method is not None
                    and splitter_method != "_split_on_whole_table"
                ):
                    batch_identifiers = ge_batch_spec.get("batch_identifiers", {})
                    partitionSpec = PartitionSpecClass(
                        partition=convert_to_string(batch_identifiers)
                    )
                sampling_method = ge_batch_spec.get("sampling_method", "")
                if sampling_method == "_sample_using_limit":
                    batchSpec.limit = ge_batch_spec["sampling_kwargs"]["n"]

                dataset_partitions.append(
                    {
                        "dataset_urn": dataset_urn,
                        "partitionSpec": partitionSpec,
                        "batchSpec": batchSpec,
                    }
                )
            elif isinstance(ge_batch_spec, RuntimeQueryBatchSpec):
                if not self.parse_table_names_from_sql:
                    warn(
                        "Enable parse_table_names_from_sql in DatahubValidationAction"
                        " config                            to try to parse the tables"
                        " being asserted from SQL query"
                    )
                    return []
                query = data_asset.batches[
                    batch_identifier
                ].batch_request.runtime_parameters["query"]
                partitionSpec = PartitionSpecClass(
                    type=PartitionTypeClass.QUERY,
                    partition=(
                        f"Query_{builder.datahub_guid(pre_json_transform(query))}"
                    ),
                )

                batchSpec = BatchSpec(
                    nativeBatchId=batch_identifier,
                    query=query,
                    customProperties=batchSpecProperties,
                )
                try:
                    tables = DefaultSQLParser(query).get_tables()
                except Exception as e:
                    logger.warning(f"Sql parser failed on {query} with {e}")
                    tables = []

                if len(set(tables)) != 1:
                    warn(
                        "DataHubValidationAction does not support cross dataset"
                        " assertions."
                    )
                    return []
                for table in tables:
                    dataset_urn = make_dataset_urn_from_sqlalchemy_uri(
                        sqlalchemy_uri,
                        None,
                        table,
                        self.env,
                        self.get_platform_instance(
                            data_asset.active_batch_definition.datasource_name
                        ),
                        self.exclude_dbname,
                        self.platform_alias,
                        self.convert_urns_to_lowercase,
                    )
                    dataset_partitions.append(
                        {
                            "dataset_urn": dataset_urn,
                            "partitionSpec": partitionSpec,
                            "batchSpec": batchSpec,
                        }
                    )
            else:
                warn(
                    "DataHubValidationAction does not recognize this GE batch spec"
                    " type- {batch_spec_type}.".format(
                        batch_spec_type=type(ge_batch_spec)
                    )
                )
        else:
            # TODO - v2-spec - SqlAlchemyDataset support
            warn(
                "DataHubValidationAction does not recognize this GE data asset type -"
                " {asset_type}. This is either using v2-api or execution engine other"
                " than sqlalchemy.".format(asset_type=type(data_asset))
            )

        return dataset_partitions
