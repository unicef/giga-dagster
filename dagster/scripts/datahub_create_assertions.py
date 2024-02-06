import json
from functools import wraps

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
    AssertionStdParameter,
    AssertionStdParameters,
    AssertionStdParameterType,
    AssertionType,
    DatasetAssertionInfo,
    DatasetAssertionScope,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import DataPlatformInstance
from datahub.metadata.com.linkedin.pegasus2avro.dataplatform import DataPlatformInfo
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetProperties
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    NumberTypeClass,
    OtherSchemaClass,
    PlatformTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)
from loguru import logger
from src.settings import settings


class CustomDatahubValidationAction:
    def __init__(self):
        self.emitter = DatahubRestEmitter(
            gms_server=settings.DATAHUB_METADATA_SERVER_URL,
            token=settings.DATAHUB_ACCESS_TOKEN,
        )
        self.data_platform_urn = builder.make_data_platform_urn(platform="deltaLake")
        self.dataset_urn = builder.make_dataset_urn(
            platform="deltaLake", name="test", env="DEV"
        )
        self.id_field_urn = builder.make_schema_field_urn(
            self.dataset_urn, field_path="id"
        )
        self.school_id_giga_field_urn = builder.make_schema_field_urn(
            self.dataset_urn, field_path="school_id_giga"
        )
        self.school_id_gov_field_urn = builder.make_schema_field_urn(
            self.dataset_urn, field_path="school_id_gov"
        )
        self.latitude_field_urn = builder.make_schema_field_urn(
            self.dataset_urn, field_path="latitude"
        )
        self.longitude_field_urn = builder.make_schema_field_urn(
            self.dataset_urn, field_path="longitude"
        )
        self.assertion_urns = [
            builder.make_assertion_urn(
                builder.make_assertion_urn("f0338dc2-985d-490c-9a95-6bf6530d024c")
            ),
            builder.make_assertion_urn(
                builder.make_assertion_urn("b2c99733-c100-4dd7-89f8-ffc9393f9cdd")
            ),
            builder.make_assertion_urn(
                builder.make_assertion_urn("42bb89e4-4dfa-4975-b35e-f30060bc908b")
            ),
            builder.make_assertion_urn(
                builder.make_assertion_urn("646dd84c-3eeb-48e4-91bb-a5846df2bb7b")
            ),
            builder.make_assertion_urn(
                builder.make_assertion_urn("6f1a7c23-56de-45bb-834b-4a8250eed8c9")
            ),
            builder.make_assertion_urn(
                builder.make_assertion_urn("0ad9d1f0-eb0c-450f-a18d-d0751af3883c")
            ),
        ]
        self.assertion_run_ids = [
            "3beb3b05-7e79-48a6-8004-3e9eb44177bb",
            "804040c9-ea8d-4cd7-98d5-2d9b60fcc025",
            "fa4a8c70-d232-4a92-8dcb-35c3547cf6e8",
            "ff54f46b-7b02-42da-b10e-9c77bbe33ef4",
            "6ce102d1-b754-456a-9bcd-8c95311138ef",
            "d92fb100-31ec-4935-b660-b485287cbf89",
        ]
        self.assertion_run_timestamps = [
            1705912435342,
            1705912436342,
            1705912437342,
            1705912438342,
            1705912439342,
            1705912440342,
        ]
        logger.info(json.dumps(self.emitter.test_connection(), indent=2))

    def __call__(self):
        self.upsert_data_platform()
        self.upsert_dataset()
        self.upsert_dataset_schema()
        self.upsert_dataset_properties()
        self.upsert_assertion()
        self.upsert_assertion_data_platform()
        self.upsert_assertion_run()

    @staticmethod
    def _log_progress(entity_type: str):
        def log_inner(func: callable):
            @wraps(func)
            def wrapper_func(self):
                logger.info(f"Creating {entity_type.lower()}...")
                func(self)
                logger.info(f"{entity_type.capitalize()} created!")

            return wrapper_func

        return log_inner

    @_log_progress("data platform")
    def upsert_data_platform(self):
        data_platform_info = DataPlatformInfo(
            name="deltaLake",
            displayName="Delta Lake",
            logoUrl="https://delta.io/static/3bd8fea55ff57287371f4714232cd4ef/ac8f8/delta-lake-logo.webp",
            datasetNameDelimiter=".",
            type=PlatformTypeClass.OTHERS,
        )
        data_platform_info_mcp = MetadataChangeProposalWrapper(
            entityUrn=self.data_platform_urn,
            aspect=data_platform_info,
        )
        self.emitter.emit_mcp(data_platform_info_mcp)

    @_log_progress("dataset")
    def upsert_dataset(self):
        dataset_properties = DatasetProperties(name="test")
        dataset_mcp = MetadataChangeProposalWrapper(
            entityUrn=self.dataset_urn, aspect=dataset_properties
        )
        self.emitter.emit_mcp(dataset_mcp)

    @_log_progress("dataset schema")
    def upsert_dataset_schema(self):
        schema = [
            {
                "fieldPath": "id",
                "type": SchemaFieldDataTypeClass(StringTypeClass()),
                "nativeDataType": "uuid4",
                "description": "Globally unique identifier",
                "nullable": False,
            },
            {
                "fieldPath": "school_id_giga",
                "type": SchemaFieldDataTypeClass(StringTypeClass()),
                "nativeDataType": "uuid3",
                "description": "Giga School ID",
                "nullable": False,
            },
            {
                "fieldPath": "school_id_gov",
                "type": SchemaFieldDataTypeClass(StringTypeClass()),
                "nativeDataType": "string",
                "description": "Government School ID",
                "nullable": False,
            },
            {
                "fieldPath": "latitude",
                "type": SchemaFieldDataTypeClass(NumberTypeClass()),
                "nativeDataType": "float",
                "description": "School latitude",
                "nullable": False,
            },
            {
                "fieldPath": "longitude",
                "type": SchemaFieldDataTypeClass(NumberTypeClass()),
                "nativeDataType": "float",
                "description": "School longitude",
                "nullable": False,
            },
        ]
        schema_properties = SchemaMetadataClass(
            schemaName="school_master",
            platform=builder.make_data_platform_urn("deltaLake"),
            platformSchema=OtherSchemaClass(rawSchema=""),
            version=0,
            hash="",
            fields=[SchemaFieldClass(**s) for s in schema],
        )
        schema_properties_mcp = MetadataChangeProposalWrapper(
            entityUrn=self.dataset_urn,
            aspect=schema_properties,
        )
        self.emitter.emit_mcp(schema_properties_mcp)

    @_log_progress("dataset properties")
    def upsert_dataset_properties(self):
        dataset_properties = DatasetPropertiesClass(
            customProperties={
                "slug": "gold/school-master/test",
            }
        )
        dataset_properties_mcp = MetadataChangeProposalWrapper(
            entityUrn=self.dataset_urn,
            aspect=dataset_properties,
        )
        self.emitter.emit_mcp(dataset_properties_mcp)

    @_log_progress("assertion")
    def upsert_assertion(self):
        assertions_info = [
            {
                "scope": DatasetAssertionScope.DATASET_COLUMN,
                "aggregation": AssertionStdAggregation.UNIQUE_PROPOTION,
                "operator": AssertionStdOperator.EQUAL_TO,
                "fields": [self.id_field_urn],
                "dataset": self.dataset_urn,
                "nativeType": "expect_ids_to_be_unique",
                "nativeParameters": {"value": "1"},
                "parameters": AssertionStdParameters(
                    value=AssertionStdParameter(
                        type=AssertionStdParameterType.NUMBER,
                        value="1",
                    )
                ),
            },
            {
                "scope": DatasetAssertionScope.DATASET_COLUMN,
                "aggregation": AssertionStdAggregation.UNIQUE_PROPOTION,
                "operator": AssertionStdOperator.EQUAL_TO,
                "fields": [
                    self.school_id_giga_field_urn,
                ],
                "dataset": self.dataset_urn,
                "nativeType": "expect_school_ids_giga_to_be_unique",
                "nativeParameters": {"value": "1"},
                "parameters": AssertionStdParameters(
                    value=AssertionStdParameter(
                        type=AssertionStdParameterType.NUMBER,
                        value="1",
                    )
                ),
            },
            {
                "scope": DatasetAssertionScope.DATASET_COLUMN,
                "aggregation": AssertionStdAggregation.UNIQUE_PROPOTION,
                "operator": AssertionStdOperator.EQUAL_TO,
                "fields": [self.school_id_gov_field_urn],
                "dataset": self.dataset_urn,
                "nativeType": "expect_school_ids_gov_to_be_unique",
                "nativeParameters": {"value": "1"},
                "parameters": AssertionStdParameters(
                    value=AssertionStdParameter(
                        type=AssertionStdParameterType.NUMBER,
                        value="1",
                    )
                ),
            },
            {
                "scope": DatasetAssertionScope.DATASET_COLUMN,
                "aggregation": AssertionStdAggregation.IDENTITY,
                "operator": AssertionStdOperator.BETWEEN,
                "fields": [self.latitude_field_urn],
                "dataset": self.dataset_urn,
                "nativeType": "expect_latitude_to_be_valid",
                "nativeParameters": {"ge": "-90", "le": "90"},
                "parameters": AssertionStdParameters(
                    minValue=AssertionStdParameter(
                        type=AssertionStdParameterType.NUMBER,
                        value="-90",
                    ),
                    maxValue=AssertionStdParameter(
                        type=AssertionStdParameterType.NUMBER,
                        value="90",
                    ),
                ),
            },
            {
                "scope": DatasetAssertionScope.DATASET_COLUMN,
                "aggregation": AssertionStdAggregation.IDENTITY,
                "operator": AssertionStdOperator.BETWEEN,
                "fields": [self.longitude_field_urn],
                "dataset": self.dataset_urn,
                "nativeType": "expect_longitude_to_be_valid",
                "nativeParameters": {"ge": "-180", "le": "180"},
                "parameters": AssertionStdParameters(
                    minValue=AssertionStdParameter(
                        type=AssertionStdParameterType.NUMBER,
                        value="-180",
                    ),
                    maxValue=AssertionStdParameter(
                        type=AssertionStdParameterType.NUMBER,
                        value="180",
                    ),
                ),
            },
            {
                "scope": DatasetAssertionScope.DATASET_COLUMN,
                "aggregation": AssertionStdAggregation.IDENTITY,
                "operator": AssertionStdOperator._NATIVE_,
                "fields": [self.school_id_giga_field_urn],
                "dataset": self.dataset_urn,
                "nativeType": "expect_valid_uuid3",
            },
        ]
        assertions = [
            AssertionInfo(
                type=AssertionType.DATASET,
                datasetAssertion=DatasetAssertionInfo(**info),
                customProperties={
                    "suite_name": "school_master_test_suite",
                },
            )
            for info in assertions_info
        ]
        assertions_mcps = [
            MetadataChangeProposalWrapper(entityUrn=urn, aspect=ass)
            for urn, ass in zip(self.assertion_urns, assertions, strict=True)
        ]
        for mcp in assertions_mcps:
            self.emitter.emit_mcp(mcp)

    @_log_progress("assertion data platform")
    def upsert_assertion_data_platform(self):
        assertion_data_platform_instance = DataPlatformInstance(
            platform=builder.make_data_platform_urn("spark")
        )
        assertion_data_platform_instance_mcps = [
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=assertion_data_platform_instance,
            )
            for urn in self.assertion_urns
        ]
        for mcp in assertion_data_platform_instance_mcps:
            self.emitter.emit_mcp(mcp)

    @_log_progress("assertion run")
    def upsert_assertion_run(self):
        assertion_unique_ids_runs = [
            AssertionRunEvent(
                timestampMillis=timestamp,
                assertionUrn=ass_urn,
                asserteeUrn=self.dataset_urn,
                runId=run_id,
                status=AssertionRunStatus.COMPLETE,
                result=AssertionResult(
                    type=AssertionResultType.SUCCESS,
                    externalUrl=settings.DATAHUB_METADATA_SERVER_URL,
                    actualAggValue=1,
                ),
            )
            for timestamp, ass_urn, run_id in zip(
                self.assertion_run_timestamps,
                self.assertion_urns,
                self.assertion_run_ids,
                strict=True,
            )
        ]
        assertion_unique_ids_run_mcps = [
            MetadataChangeProposalWrapper(entityUrn=urn, aspect=run)
            for urn, run in zip(
                self.assertion_urns, assertion_unique_ids_runs, strict=True
            )
        ]
        for mcp in assertion_unique_ids_run_mcps:
            self.emitter.emit_mcp(mcp)


if __name__ == "__main__":
    run_validation = CustomDatahubValidationAction()
    run_validation()
