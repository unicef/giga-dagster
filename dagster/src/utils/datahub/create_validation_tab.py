import json
import uuid
from datetime import datetime
from functools import wraps

import loguru
from datahub.emitter import mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.assertion import (
    AssertionInfo,
    AssertionResult,
    AssertionResultType,
    AssertionRunEvent,
    AssertionRunStatus,
    AssertionStdAggregation,
    AssertionStdOperator,
    AssertionType,
    DatasetAssertionInfo,
    DatasetAssertionScope,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import DataPlatformInstance
from src.sensors.base import FileConfig
from src.settings import settings
from src.utils.adls import ADLSFileClient

from dagster import OpExecutionContext


class EmitDatasetAssertionResults:
    def __init__(
        self,
        dq_summary_statistics: dict,
        context: OpExecutionContext = None,
    ):
        self.emitter = DatahubRestEmitter(
            gms_server=settings.DATAHUB_METADATA_SERVER_URL,
            token=settings.DATAHUB_ACCESS_TOKEN,
        )
        self.context = context
        config = FileConfig(**context.get_step_execution_context().op_config)
        self.dataset_urn = config.datahub_destination_dataset_urn

        dq_summary_statistics.pop("summary")
        self.dq_summary_statistics = []
        for value in dq_summary_statistics.values():
            self.dq_summary_statistics.extend(value)
        self.logger.info(json.dumps(self.emitter.test_connection(), indent=2))

    def __call__(self):
        self.upsert_validation_tab()

    @staticmethod
    def _log_progress(entity_type: str):
        def log_inner(func: callable):
            @wraps(func)
            def wrapper_func(self):
                self.logger.info(f"Creating {entity_type.lower()}...")
                func(self)
                self.logger.info(f"{entity_type.capitalize()} created!")

            return wrapper_func

        return log_inner

    @property
    def logger(self):
        if self.context is None:
            return loguru.logger
        return self.context.log

    @staticmethod
    def extract_assertion_info_of_column(column_dq_result: dict, dataset_urn: str):
        field_urn = builder.make_schema_field_urn(
            dataset_urn, field_path=column_dq_result["column"]
        )

        info = {
            "scope": DatasetAssertionScope.DATASET_COLUMN,
            "aggregation": AssertionStdAggregation.IDENTITY,
            "operator": AssertionStdOperator._NATIVE_,
            "fields": [field_urn],
            "dataset": dataset_urn,
            "nativeType": column_dq_result["assertion"],
        }

        assertion_of_column = AssertionInfo(
            type=AssertionType.DATASET,
            datasetAssertion=DatasetAssertionInfo(**info),
            customProperties={
                "suite_name": "school_master_test_suite",
            },
        )

        return assertion_of_column

    @_log_progress("validation tab")
    def upsert_validation_tab(self):
        for column_dq_result in self.dq_summary_statistics:
            if not column_dq_result.get("column"):
                # TODO: Might need to do something else with checks that aren't specific to a column
                continue

            self.logger.info(
                f"Creating assertion info for column: {column_dq_result['column']}..."
            )

            col_assertion_info = self.extract_assertion_info_of_column(
                column_dq_result=column_dq_result, dataset_urn=self.dataset_urn
            )
            assertion_urn = builder.make_assertion_urn(
                f"{column_dq_result['assertion']}_col_{column_dq_result['column']}"
            )
            assertion_data_platform_instance = DataPlatformInstance(
                platform=builder.make_data_platform_urn("spark")
            )
            assertion_run = AssertionRunEvent(
                timestampMillis=int(datetime.now().timestamp() * 1000),
                assertionUrn=assertion_urn,
                asserteeUrn=self.dataset_urn,
                runId=str(uuid.uuid4()),
                status=AssertionRunStatus.COMPLETE,
                result=AssertionResult(
                    type=AssertionResultType.SUCCESS,
                    externalUrl=settings.DATAHUB_METADATA_SERVER_URL,
                    nativeResults={
                        str(k): str(v) for (k, v) in column_dq_result.items()
                    },
                ),
            )

            # TODO: Figure out how to emit this by batch; check out MetadataChangeProposalWrapper.construct_many()
            assertion_mcp = MetadataChangeProposalWrapper(
                entityUrn=assertion_urn, aspect=col_assertion_info
            )
            assertion_run_mcp = MetadataChangeProposalWrapper(
                entityUrn=assertion_urn, aspect=assertion_run
            )
            assertion_data_platform_mcp = MetadataChangeProposalWrapper(
                entityUrn=assertion_urn, aspect=assertion_data_platform_instance
            )
            try:
                self.emitter.emit_mcp(assertion_mcp)
                self.emitter.emit_mcp(assertion_run_mcp)
                self.emitter.emit_mcp(assertion_data_platform_mcp)
            except Exception as error:
                self.context.log.info(f"ERROR on Assertion Run: {error}")

        self.logger.info("DATASET VALIDATION TAB HAS BEEN CREATED IN DATAHUB")


if __name__ == "__main__":
    adls = ADLSFileClient()
    dq_results_filepath = "logs-gx/school-coverage-data/randomstring_BIH_school-coverage_itu_20240227.json"
    dq_summary_statistics = adls.download_json(dq_results_filepath)
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:adls,adls-testing-raw.school-coverage-data.BIH_school_coverage_test_pipeline_correct_schema,DEV)"

    emit_assertions = EmitDatasetAssertionResults(
        dataset_urn=dataset_urn, dq_summary_statistics=dq_summary_statistics
    )
    emit_assertions()
