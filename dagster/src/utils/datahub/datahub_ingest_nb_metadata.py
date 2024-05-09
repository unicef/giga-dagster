import json
from datetime import datetime
from functools import wraps
from zoneinfo import ZoneInfo

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataplatform import DataPlatformInfo
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetProperties
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    PlatformTypeClass,
)
from loguru import logger

from src.settings import settings
from src.utils.github_api_calls import list_ipynb_from_github_repo


class NotebookIngestionAction:
    def __init__(self, notebook_metadata: dict) -> None:
        self.emitter = DatahubRestEmitter(
            gms_server=settings.DATAHUB_METADATA_SERVER_URL,
            token=settings.DATAHUB_ACCESS_TOKEN,
            retry_max_times=5,
        )
        self.data_platform_urn = builder.make_data_platform_urn(platform="github")
        self.notebook_metadata = notebook_metadata
        self.notebook_name = self.notebook_metadata["filename"]
        self.dataset_urn = builder.make_dataset_urn(
            platform="github",
            name=self.notebook_name,
            env=settings.ADLS_ENVIRONMENT,
        )
        logger.info(json.dumps(self.emitter.test_connection(), indent=2))

    def __call__(self) -> None:
        self.upsert_data_platform()
        self.upsert_dataset()
        self.upsert_dataset_properties()

    @staticmethod
    def _log_progress(entity_type: str) -> callable:
        def log_inner(func: callable) -> callable:
            @wraps(func)
            def wrapper_func(self) -> None:
                logger.info(f"Creating {entity_type.lower()}...")
                func(self)
                logger.info(f"{entity_type.capitalize()} created!")

            return wrapper_func

        return log_inner

    @_log_progress("data platform")
    def upsert_data_platform(self) -> None:
        data_platform_info = DataPlatformInfo(
            name="github",
            displayName="Github",
            logoUrl="https://github.githubassets.com/assets/GitHub-Mark-ea2971cee799.png",
            datasetNameDelimiter=".",
            type=PlatformTypeClass.OTHERS,
        )
        data_platform_info_mcp = MetadataChangeProposalWrapper(
            entityUrn=self.data_platform_urn,
            aspect=data_platform_info,
        )
        self.emitter.emit_mcp(data_platform_info_mcp)

    @_log_progress("dataset")
    def upsert_dataset(self) -> None:
        dataset_properties = DatasetProperties(
            name=self.notebook_name,
        )
        dataset_mcp = MetadataChangeProposalWrapper(
            entityUrn=self.dataset_urn,
            aspect=dataset_properties,
        )
        self.emitter.emit_mcp(dataset_mcp)

    @_log_progress("dataset properties")
    def upsert_dataset_properties(self) -> None:
        # current date and time format: day/month/year 24-hour clock
        now = datetime.now(tz=ZoneInfo("UTC")).isoformat()
        dataset_properties = DatasetPropertiesClass(
            customProperties=self.notebook_metadata | {"last_ingestion_time": now},
        )
        dataset_properties_mcp = MetadataChangeProposalWrapper(
            entityUrn=self.dataset_urn,
            aspect=dataset_properties,
        )
        self.emitter.emit_mcp(dataset_properties_mcp)


if __name__ == "__main__":
    owner = "unicef"
    repo = "coverage_workflow"
    path = "Notebooks/"
    notebook_metadata_list = list_ipynb_from_github_repo(owner, repo, path)

    for notebook_metadata in notebook_metadata_list:
        run_notebook_ingestion = NotebookIngestionAction(
            notebook_metadata=notebook_metadata,
        )
        run_notebook_ingestion()
