from country_converter import CountryConverter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

from src.settings import settings


def set_tag_mutation_query(country_name):
    query = f"""
        mutation {{
            createTag(input:{{
                id: "{country_name}",
                name: "{country_name}",
                description: "country name"
                }})
            }}
    """
    return query


def create_tags():
    coco = CountryConverter()
    country_list = list(coco.data["name_short"])

    datahub_graph_client = DataHubGraph(
        DatahubClientConfig(
            server=settings.DATAHUB_METADATA_SERVER_URL,
            token=settings.DATAHUB_ACCESS_TOKEN,
        )
    )

    for country_name in country_list:
        query = set_tag_mutation_query(country_name=country_name)
        try:
            datahub_graph_client.execute_graphql(query=query)
        except Exception:
            pass
