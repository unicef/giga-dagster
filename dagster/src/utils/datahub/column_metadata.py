from models.file_upload import FileUpload
from sqlalchemy import select

from dagster import OpExecutionContext
from src.schemas.file_upload import FileUploadConfig
from src.utils.datahub.graphql import (
    datahub_graph_client,
    execute_batch_mutation,
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


def add_column_metadata(
    dataset_urn: str,
    column_licenses: dict[str, str] = None,
    column_descriptions: dict[str, str] = None,
    context: OpExecutionContext = None,
) -> None:
    logger = get_context_with_fallback_logger(context)
    datahub_schema_fields = datahub_graph_client.get_schema_metadata(dataset_urn).fields
    datahub_column_names = [field.fieldPath for field in datahub_schema_fields]

    # COLUMN LICENSES
    logger.info("DATAHUB: ADD COLUMN LICENSES...")
    if column_licenses is not None:
        queries = ""
        for column in datahub_column_names:
            if column in column_licenses.keys():
                license = column_licenses[column]
                base_query = column_tag_query(
                    tag_key=license,
                    column=column,
                    dataset_urn=dataset_urn,
                )
                queries = queries + " " + f"update_{column}: {base_query}"
        execute_batch_mutation(queries, context)
    else:
        context.log.warning("No column licenses to emit.")

    # COLUMN DESCRIPTIONS
    logger.info("DATAHUB: ADD COLUMN DESCRIPTIONS...")
    if column_descriptions is not None:
        queries = ""
        for column in datahub_column_names:
            if column in column_descriptions.keys():
                description = column_descriptions[column]
                if description is not None:
                    base_query = column_description_query(
                        column=column,
                        dataset_urn=dataset_urn,
                        description=description,
                    )
                    queries = queries + " " + f"update_{column}: {base_query}"
        execute_batch_mutation(queries, context)
    else:
        context.log.warning("No column descriptions to emit.")


if __name__ == "__main__":
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:adlsGen2,bronze/school-geolocation/l2wkbpxgyts291f0au9pyh6p_BEN_geolocation_20240321-130111,DEV)"
    # COLUMN LICENSES
    column_license_dict = {
        "education_level": "Giga Analysis",
        "education_level_govt": "CC-BY-4.0",
        "connectivity_govt": "CC-BY-4.0",
    }
    # COLUMN DESCRIPTIONS
    column_desc_dict = {
        "education_level": "Description: educ_level",
        "education_level_govt": "Description: educ_level_govt",
        "connectivity_govt": "Description: conn_govt",
    }
    add_column_metadata(
        dataset_urn=dataset_urn,
        column_licenses=column_license_dict,
        column_descriptions=column_desc_dict,
    )
