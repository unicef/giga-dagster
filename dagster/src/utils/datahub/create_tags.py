from country_converter import CountryConverter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from src.settings import settings
from src.utils.logger import get_context_with_fallback_logger

from dagster import OpExecutionContext


def createTag_query(tag_key: str, display_name: str, description: str = ""):
    query = f"""
        mutation {{
            createTag(input:{{
                id: "{tag_key}",
                name: "{display_name}",
                description: "{description}"
                }})
            }}
    """
    return query


def updateTag_query(
    tag_key: str,
    new_display_name: str,
    new_description: str = "",
):
    query = f"""
        mutation {{
            updateTag(
                urn:"urn:li:tag:{tag_key}",
                input: {{
                    urn: "urn:li:tag:{tag_key}",
                    name: "{new_display_name}",
                    description: "{new_description}"
                }}
            ){{
                urn
            }}
        }}"""
    return query


def tag_mutation_with_exception(
    tag_key: str,
    display_name: str,
    description: str = "",
    context: OpExecutionContext = None,
):
    datahub_graph_client = DataHubGraph(
        DatahubClientConfig(
            server=settings.DATAHUB_METADATA_SERVER_URL,
            token=settings.DATAHUB_ACCESS_TOKEN,
        )
    )
    try:
        query = updateTag_query(
            tag_key=tag_key, new_display_name=display_name, new_description=description
        )
        datahub_graph_client.execute_graphql(query=query)
    except Exception as error:
        get_context_with_fallback_logger(context).error(error)
        try:
            query = createTag_query(
                tag_key=tag_key, display_name=display_name, description=description
            )
            datahub_graph_client.execute_graphql(query=query)
        except Exception:
            get_context_with_fallback_logger(context).error(error)


def create_tags(context: OpExecutionContext = None):
    coco = CountryConverter()
    country_list = list(coco.data["name_short"])
    for country_name in country_list:
        tag_mutation_with_exception(
            tag_key=country_name,
            display_name=f"Country: {country_name}",
            context=context,
        )

    for license in settings.LICENSE_LIST:
        tag_mutation_with_exception(
            tag_key=license, display_name=f"License: {license}", context=context
        )


if __name__ == "__main__":
    pass
