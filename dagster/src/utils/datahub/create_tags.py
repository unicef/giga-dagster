from country_converter import CountryConverter

from dagster import OpExecutionContext
from src.settings import settings
from src.utils.datahub.graphql import datahub_graph_client
from src.utils.logger import get_context_with_fallback_logger


def create_tag_query(tag_key: str, display_name: str, description: str = "") -> str:
    return f"""
        mutation {{
            createTag(input:{{
                id: "{tag_key}",
                name: "{display_name}",
                description: "{description}"
                }})
            }}
    """


def update_tag_query(
    tag_key: str,
    new_display_name: str,
    new_description: str = "",
) -> str:
    return f"""
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


def tag_mutation_with_exception(
    tag_key: str,
    display_name: str,
    description: str = "",
    context: OpExecutionContext = None,
) -> None:
    try:
        query = update_tag_query(
            tag_key=tag_key,
            new_display_name=display_name,
            new_description=description,
        )
        datahub_graph_client.execute_graphql(query=query)
    except Exception as error:
        get_context_with_fallback_logger(context).error(error)
        try:
            query = create_tag_query(
                tag_key=tag_key,
                display_name=display_name,
                description=description,
            )
            datahub_graph_client.execute_graphql(query=query)
        except Exception as error:
            get_context_with_fallback_logger(context).error(error)


def create_tags(context: OpExecutionContext = None) -> None:
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
            tag_key=license,
            display_name=f"License: {license}",
            context=context,
        )


if __name__ == "__main__":
    pass
