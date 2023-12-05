from urllib import parse

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

from src.settings import settings


def list_allgroups_query():
    query = """
        query {
            listGroups(input: {}) {
                groups {
                    urn
                    name
                }
            }
        }
    """

    return query


def policy_mutation_query(country_name, group_urn):
    datasets_urns_list = list_datasets_by_tag(tag=country_name)

    query = f"""
    mutation {{
        updatePolicy(
            urn: "urn:li:dataHubPolicy:{country_name}-viewer",
            input: {{
                type: METADATA,
                name: "{country_name} - VIEWER",
                state: ACTIVE,
                description: "Members can view datasets with country name: {country_name}.",
                resources: {{
                    type: "",
                    resources: {datasets_urns_list},
                    allResources: true,
                    filter: {{
                        criteria [{{
                            field: "",
                            values: [""],
                            condition: EQUALS
                        }}]
                    }}
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


def list_datasets_by_tag(tag):
    datahub_graph_client = DataHubGraph(
        DatahubClientConfig(
            server=settings.DATAHUB_METADATA_SERVER_URL,
            token=settings.DATAHUB_ACCESS_TOKEN,
        )
    )

    search_query = f"""query {{
        search (input:{{
            type: DATASET,
            query: "tag:{tag}",
        }}) {{
            searchResults {{
                entity {{
                    urn
                }}
            }}
        }}
    }}"""

    search_results = datahub_graph_client.execute_graphql(query=search_query)
    results = search_results["search"]["searchResults"]

    urn_list = []
    for result in results:
        urn_list.append(result["entity"]["urn"])

    return urn_list


def update_policies():
    datahub_graph_client = DataHubGraph(
        DatahubClientConfig(
            server=settings.DATAHUB_METADATA_SERVER_URL,
            token=settings.DATAHUB_ACCESS_TOKEN,
        )
    )

    groups_list = datahub_graph_client.execute_graphql(query=list_allgroups_query())

    for group in groups_list["listGroups"]["groups"]:
        group_urn = group["urn"]
        country_name = parse.unquote(group["name"])

        ### WIP: Will add a valid country name checker ###
        query = policy_mutation_query(country_name=country_name, group_urn=group_urn)
        datahub_graph_client.execute_graphql(query=query)
        print(f"Policy of {country_name} - VIEWER updated successfully.")

    return


# update_policies()
# print(list_datasets_by_tag(tag='Benin'))
