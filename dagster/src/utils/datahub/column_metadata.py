from models.file_upload import FileUpload
from sqlalchemy import select

from dagster import OpExecutionContext
from src.schemas.file_upload import FileUploadConfig
from src.utils.datahub.graphql import (
    execute_batch_mutation,
    get_datahub_graph_client,
)
from src.utils.db.primary import get_db_context
from src.utils.logger import get_context_with_fallback_logger
from src.utils.op_config import FileConfig


def get_column_licenses(config: FileConfig) -> dict[str, str]:
    with get_db_context() as db:
        file_upload = db.scalar(
            select(FileUpload).where(FileUpload.id == config.filename_components.id),
        )
        if file_upload is None:
            raise FileNotFoundError(
                f"Database entry for FileUpload with id `{config.filename_components.id}` was not found",
            )

        file_upload = FileUploadConfig.from_orm(file_upload)

    return file_upload.column_license


def column_tag_query(tag_key: str, column: str, dataset_urn: str) -> str:
    return f"""
            addTag(input:{{
                tagUrn: "urn:li:tag:{tag_key}",
                resourceUrn: "{dataset_urn}",
                subResource: "{column}",
                subResourceType: DATASET_FIELD
            }})
        """


def column_description_query(dataset_urn: str, column: str, description: str) -> str:
    return f"""
            updateDescription(input:{{
                description: "{description}",
                resourceUrn: "{dataset_urn}",
                subResource: "{column}",
                subResourceType: DATASET_FIELD
            }})
        """


def _build_license_queries(
    datahub_column_names: list[str],
    column_licenses: dict[str, str],
    dataset_urn: str,
) -> str:
    queries = ""
    for column in datahub_column_names:
        if column in column_licenses:
            license_tag = column_licenses[column]
            base_query = column_tag_query(
                tag_key=license_tag,
                column=column,
                dataset_urn=dataset_urn,
            )
            queries += f" update_{column}: {base_query}"
    return queries


def _build_description_queries(
    datahub_column_names: list[str],
    column_descriptions: dict[str, str],
    dataset_urn: str,
) -> str:
    queries = ""
    for column in datahub_column_names:
        description = column_descriptions.get(column)
        if description is not None:
            base_query = column_description_query(
                column=column,
                dataset_urn=dataset_urn,
                description=description,
            )
            queries += f" update_{column}: {base_query}"
    return queries


def add_column_metadata(
    dataset_urn: str,
    column_licenses: dict[str, str] = None,
    column_descriptions: dict[str, str] = None,
    context: OpExecutionContext = None,
) -> None:
    logger = get_context_with_fallback_logger(context)
    client = get_datahub_graph_client()
    if client is None:
        logger.warning("DataHub is not configured. Skipping column metadata.")
        return

    datahub_schema_fields = client.get_schema_metadata(dataset_urn).fields
    datahub_column_names = [field.fieldPath for field in datahub_schema_fields]

    # COLUMN LICENSES
    logger.info("DATAHUB: ADD COLUMN LICENSES...")
    if column_licenses:
        queries = _build_license_queries(
            datahub_column_names, column_licenses, dataset_urn
        )
        if queries:
            execute_batch_mutation(queries, context)
        else:
            logger.warning("No column licenses to emit.")
    else:
        logger.warning("No column licenses to emit.")

    # COLUMN DESCRIPTIONS
    logger.info("DATAHUB: ADD COLUMN DESCRIPTIONS...")
    if column_descriptions:
        queries = _build_description_queries(
            datahub_column_names, column_descriptions, dataset_urn
        )
        if queries:
            execute_batch_mutation(queries, context)
        else:
            logger.warning("No column descriptions to emit.")
    else:
        logger.warning("No column descriptions to emit.")


if __name__ == "__main__":
    pass
