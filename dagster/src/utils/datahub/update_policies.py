import json
from urllib import parse

import country_converter as cc
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from src.settings import settings
from src.utils.logger import get_context_with_fallback_logger


def policy_mutation_query(group_urn):
    group_name = parse.unquote(group_urn.split("urn:li:corpGroup:")[1])
    country_name = group_name.split("-")[0]
    dataset_type = group_name.split(" ")[1].lower()
    datasets_urns_list = list_datasets_by_filter(
        tag=country_name, dataset_type=dataset_type
    )

    query = f"""
    mutation {{
        updatePolicy(
            urn: "urn:li:dataHubPolicy:{group_name}-viewer",
            input: {{
                type: METADATA,
                name: "{group_name} - VIEWER",
                state: ACTIVE,
                description: "Members can view datasets with country name: {group_name}.",
                resources: {{
                    resources: {datasets_urns_list},
                    allResources: true,
                }},
                privileges: ["VIEW_ENTITY_PAGE", "VIEW_DATASET_USAGE", "VIEW_DATASET_PROFILE"],
                actors: {{
                    groups: ["{group_urn}"],
                    resourceOwners: true,
                    allUsers: false,
                    allGroups: false
                }}
        }})
    }}
    """

    return query


def create_policy_query(group_urn):
    group_name = parse.unquote(group_urn.split("urn:li:corpGroup:")[1])
    country_name = group_urn.split("urn:li:corpGroup:")[1].split("-")[0]
    dataset_type = group_name.split(" ")[1].lower()
    datasets_urns_list = list_datasets_by_filter(
        tag=country_name, dataset_type=dataset_type
    )

    query = f"""
        mutation {{
            createPolicy(
                input: {{
                    type: METADATA,
                    name: "{group_name} - VIEWER",
                    state: ACTIVE,
                    description: "Members can view datasets in {group_name}.",
                    resources: {{
                        resources: {datasets_urns_list},
                        allResources: true,
                    }},
                    privileges: ["VIEW_ENTITY_PAGE", "VIEW_DATASET_USAGE", "VIEW_DATASET_PROFILE"],
                    actors: {{
                        groups: ["{group_urn}"],
                        resourceOwners: true,
                        allUsers: false,
                        allGroups: false
                    }}
            }})
        }}
        """
    return query


def list_datasets_by_filter(tag: str, dataset_type: str):
    datahub_graph_client = DataHubGraph(
        DatahubClientConfig(
            server=settings.DATAHUB_METADATA_SERVER_URL,
            token=settings.DATAHUB_ACCESS_TOKEN,
        )
    )
    query = f"tag:{tag}"
    dataset_urns_iterator = datahub_graph_client.get_urns_by_filter(
        entity_types=["dataset"],
        query=query,
        extraFilters=[
            {"field": "urn", "values": [dataset_type], "condition": "CONTAIN"}
        ],
    )
    urn_list = list(dataset_urns_iterator)
    return json.dumps(urn_list)


def group_urns_iterator():
    datahub_graph_client = DataHubGraph(
        DatahubClientConfig(
            server=settings.DATAHUB_METADATA_SERVER_URL,
            token=settings.DATAHUB_ACCESS_TOKEN,
        )
    )
    return datahub_graph_client.get_urns_by_filter(entity_types=["corpGroup"])


def is_valid_country_name(country_name):
    coco = cc.CountryConverter()
    country_list = list(coco.data["name_short"])
    return country_name in country_list


def update_policies(context=None):
    datahub_graph_client = DataHubGraph(
        DatahubClientConfig(
            server=settings.DATAHUB_METADATA_SERVER_URL,
            token=settings.DATAHUB_ACCESS_TOKEN,
        )
    )

    for group_urn in group_urns_iterator():
        country_name = group_urn.split("urn:li:corpGroup:")[1].split("-")[0]
        if is_valid_country_name(country_name):
            try:
                query = policy_mutation_query(group_urn=group_urn)
                get_context_with_fallback_logger(context).info(
                    "UPDATING DATAHUB POLICY..."
                )
                get_context_with_fallback_logger(context).info(query)
                datahub_graph_client.execute_graphql(query=query)
            except Exception as error:
                get_context_with_fallback_logger(context).error(error)
                try:
                    query = create_policy_query(group_urn=group_urn)
                    get_context_with_fallback_logger(context).info(
                        "CREATING DATAHUB POLICY..."
                    )
                    get_context_with_fallback_logger(context).info(query)
                    datahub_graph_client.execute_graphql(query=query)
                except Exception:
                    get_context_with_fallback_logger(context).error(error)


def update_specific_policy(group_urn: str, context=None):
    datahub_graph_client = DataHubGraph(
        DatahubClientConfig(
            server=settings.DATAHUB_METADATA_SERVER_URL,
            token=settings.DATAHUB_ACCESS_TOKEN,
        )
    )

    country_name = group_urn.split("urn:li:corpGroup:")[1].split("-")[0]
    if is_valid_country_name(country_name):
        try:
            query = policy_mutation_query(group_urn=group_urn)
            get_context_with_fallback_logger(context).info("UPDATING DATAHUB POLICY...")
            get_context_with_fallback_logger(context).info(query)
            datahub_graph_client.execute_graphql(query=query)
        except Exception as error:
            get_context_with_fallback_logger(context).error(error)
            try:
                query = create_policy_query(group_urn=group_urn)
                get_context_with_fallback_logger(context).info(
                    "CREATING DATAHUB POLICY..."
                )
                get_context_with_fallback_logger(context).info(query)
                datahub_graph_client.execute_graphql(query=query)
            except Exception:
                get_context_with_fallback_logger(context).error(error)


if __name__ == "__main__":
    print(list_datasets_by_filter(tag="Benin", dataset_type="geolocation"))
    # print(list_all_groups())
    pass
