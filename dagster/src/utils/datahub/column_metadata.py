from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from models.file_upload import FileUpload
from sqlalchemy import select

from dagster import OpExecutionContext
from src.schemas.file_upload import FileUploadConfig
from src.settings import settings
from src.utils.db import get_db_context
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


def add_column_tag_query(tag_key: str, column: str, dataset_urn: str) -> str:
    return f"""
        mutation {{
            addTag(input:{{
                tagUrn: "urn:li:tag:{tag_key}",
                resourceUrn: "{dataset_urn}",
                subResource: "{column}",
                subResourceType: DATASET_FIELD
            }})
        }}"""


def add_column_description_query(
    dataset_urn: str, column: str, description: str
) -> str:
    return f"""
        mutation {{
            updateDescription(input:{{
                description: "{description}",
                resourceUrn: "{dataset_urn}",
                subResource: "{column}",
                subResourceType: DATASET_FIELD
            }})
        }}"""


def add_column_metadata(
    dataset_urn: str,
    column_licenses: dict[str, str],
    column_descriptions: list[dict],
    context: OpExecutionContext = None,
) -> None:
    datahub_graph_client = DataHubGraph(
        DatahubClientConfig(
            server=settings.DATAHUB_METADATA_SERVER_URL,
            token=settings.DATAHUB_ACCESS_TOKEN,
        ),
    )
    logger = get_context_with_fallback_logger(context)
    # COLUMN LICENSES
    for column, license in column_licenses.items():
        try:
            query = add_column_tag_query(
                tag_key=license,
                column=column,
                dataset_urn=dataset_urn,
            )
            datahub_graph_client.execute_graphql(query=query)
        except Exception as error:
            logger.warning(error)
            logger.warning(
                "Expected error if due to a missing field. Some transforms drop original columns."
            )
            continue

    # COLUMN DESCRIPTIONS
    for item in column_descriptions:
        try:
            column = item["column"]
            description = item["description"]
            query = add_column_description_query(
                column=column,
                dataset_urn=dataset_urn,
                description=description,
            )
            datahub_graph_client.execute_graphql(query=query)
        except Exception as error:
            logger.warning(error)
            logger.warning(
                "Expected error if due to a missing field. Some transforms drop original columns."
            )
            continue


if __name__ == "__main__":
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:adlsGen2,bronze/school-geolocation/l2wkbpxgyts291f0au9pyh6p_BEN_geolocation_20240321-130111,DEV)"
    datahub_graph_client = DataHubGraph(
        DatahubClientConfig(
            server=settings.DATAHUB_METADATA_SERVER_URL,
            token=settings.DATAHUB_ACCESS_TOKEN,
        ),
    )
    # COLUMN LICENSES
    column_license_dict = {
        "education_level": "Giga Analysis",
        "education_level_govt": "CC-BY-4.0",
        "connectivity_govt": "CC-BY-4.0",
    }
    for column, license in column_license_dict.items():
        query = add_column_tag_query(
            tag_key=license,
            column=column,
            dataset_urn=dataset_urn,
        )
        datahub_graph_client.execute_graphql(query=query)

    # COLUMN DESCRIPTIONS
    column_desc_dict = {
        "education_level": "Description: educ_level",
        "education_level_govt": "Description: educ_level_govt",
        "connectivity_govt": "Description: conn_govt",
    }
    for column, desc in column_desc_dict.items():
        query = add_column_description_query(
            column=column,
            dataset_urn=dataset_urn,
            description=desc,
        )
        datahub_graph_client.execute_graphql(query=query)
