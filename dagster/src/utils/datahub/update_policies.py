import json
from urllib import parse

import country_converter as cc
import sentry_sdk
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

from dagster import OpExecutionContext
from src.settings import settings
from src.utils.datahub.builders import build_group_urn
from src.utils.datahub.identify_country_name import identify_country_name
from src.utils.logger import get_context_with_fallback_logger
from src.utils.op_config import FileConfig
from src.utils.sentry import log_op_context


def policy_mutation_query(group_urn: str) -> str:
    return f"""
    mutation {{
        {update_policy_base_query(group_urn)}
    }}
    """


def update_policy_base_query(group_urn: str) -> str:
    group_name = parse.unquote(group_urn.split("urn:li:corpGroup:")[1])
    country_name = group_name.split("-")[0]
    dataset_type = group_name.split(" ")[1].lower()
    datasets_urns_list = list_datasets_by_filter(
        tag=country_name, dataset_type=dataset_type
    )

    return f"""
        updatePolicy(
            urn: "urn:li:dataHubPolicy:{group_name}-viewer",
            input: {{
                type: METADATA,
                name: "{group_name} - VIEWER",
                state: ACTIVE,
                description: "Members can view {dataset_type} datasets with country name: {country_name}.",
                resources: {{
                    resources: {datasets_urns_list},
                    allResources: false,
                }},
                privileges: ["VIEW_ENTITY_PAGE", "VIEW_DATASET_USAGE", "VIEW_DATASET_PROFILE"],
                actors: {{
                    groups: ["{group_urn}"],
                    resourceOwners: true,
                    allUsers: false,
                    allGroups: false
                }}
        }})
    """


def create_policy_query(group_urn: str) -> str:
    group_name = parse.unquote(group_urn.split("urn:li:corpGroup:")[1])
    country_name = group_name.split("-")[0]
    dataset_type = group_name.split(" ")[1].lower()
    datasets_urns_list = list_datasets_by_filter(
        tag=country_name, dataset_type=dataset_type
    )

    return f"""
        mutation {{
            createPolicy(
                input: {{
                    type: METADATA,
                    name: "{group_name} - VIEWER",
                    state: ACTIVE,
                    description: "Members can view {dataset_type} datasets with country name: {country_name}.",
                    resources: {{
                        resources: {datasets_urns_list},
                        allResources: false,
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


def list_datasets_by_filter(tag: str, dataset_type: str) -> str:
    datahub_graph_client = DataHubGraph(
        DatahubClientConfig(
            server=settings.DATAHUB_METADATA_SERVER_URL,
            token=settings.DATAHUB_ACCESS_TOKEN,
            retry_max_times=5,
            retry_status_codes=[
                403,
                429,
                500,
                502,
                503,
                504,
            ],
        ),
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
    return json.dumps(
        urn_list
    )  # Puts list items in double quotes # GraphQL does not allow single quotes


def group_urns_iterator():
    datahub_graph_client = DataHubGraph(
        DatahubClientConfig(
            server=settings.DATAHUB_METADATA_SERVER_URL,
            token=settings.DATAHUB_ACCESS_TOKEN,
            retry_max_times=5,
            retry_status_codes=[
                403,
                429,
                500,
                502,
                503,
                504,
            ],
        )
    )
    return datahub_graph_client.get_urns_by_filter(entity_types=["corpGroup"])


def is_valid_country_name(country_name: str) -> bool:
    coco = cc.CountryConverter()
    country_list = list(coco.data["name_short"])
    return country_name in country_list


def update_policies(context: OpExecutionContext = None) -> None:
    datahub_graph_client = DataHubGraph(
        DatahubClientConfig(
            server=settings.DATAHUB_METADATA_SERVER_URL,
            token=settings.DATAHUB_ACCESS_TOKEN,
            retry_max_times=5,
            retry_status_codes=[
                403,
                429,
                500,
                502,
                503,
                504,
            ],
        )
    )
    logger = get_context_with_fallback_logger(context)
    queries = ""
    for i, group_urn in enumerate(group_urns_iterator()):
        country_name = parse.unquote(
            group_urn.split("urn:li:corpGroup:")[1].split("-")[0]
        )
        if is_valid_country_name(country_name):
            base_query = update_policy_base_query(group_urn=group_urn)
            queries = queries + " " + f"update{i}: {base_query}"
        else:
            warning_message = f"INVALID COUNTRY NAME: {country_name}. No Datahub Policy is created/updated for this role."
            logger.warning(warning_message)
            log_op_context(context)
            sentry_sdk.capture_message(warning_message)

    batch_mutation_query = f"""
        mutation {{
            {queries}
        }}
        """
    try:
        logger.info("UPDATING DATAHUB POLICIES...")
        logger.info(batch_mutation_query)
        graphql_execution = datahub_graph_client.execute_graphql(
            query=batch_mutation_query
        )
        logger.info(graphql_execution)
        logger.info("DATAHUB POLICIES UPDATED SUCCESSFULLY.")
    except Exception as error:
        logger.error(error)
        log_op_context(context)
        sentry_sdk.capture_exception(error=error)


def update_policy_for_group(
    config: FileConfig, context: OpExecutionContext = None
) -> None:
    logger = get_context_with_fallback_logger(context)
    datahub_graph_client = DataHubGraph(
        DatahubClientConfig(
            server=settings.DATAHUB_METADATA_SERVER_URL,
            token=settings.DATAHUB_ACCESS_TOKEN,
            retry_max_times=5,
            retry_status_codes=[
                403,
                429,
                500,
                502,
                503,
                504,
            ],
        )
    )
    country_code = config.country_code
    country_name = identify_country_name(country_code=country_code)
    domain = config.domain
    dataset_type = config.dataset_type
    group_urn = build_group_urn(
        country_name=country_name, dataset_type=dataset_type, domain=domain
    )
    logger.info(f"policy group urn: {group_urn}")
    update_policy_base(
        group_urn=group_urn, datahub_graph_client=datahub_graph_client, context=context
    )


def update_policy_base(
    group_urn: str,
    datahub_graph_client: DataHubGraph[DatahubClientConfig],
    context: OpExecutionContext = None,
) -> None:
    logger = get_context_with_fallback_logger(context)

    country_name = parse.unquote(group_urn.split("urn:li:corpGroup:")[1].split("-")[0])
    if is_valid_country_name(country_name):
        try:
            query = policy_mutation_query(group_urn=group_urn)
            logger.info(f"UPDATING DATAHUB POLICY: {group_urn}...")
            logger.info(query)
            datahub_graph_client.execute_graphql(query=query)
            logger.info("DATAHUB POLICY UPDATED SUCCESSFULLY.")
        except Exception as error:
            logger.error(error)
            sentry_sdk.capture_exception(error=error)
            if context is not None:
                log_op_context(context)
    else:
        warning_message = f"INVALID COUNTRY NAME: {country_name}. No Datahub Policy is created/updated for this role."
        logger.warning(warning_message)
        sentry_sdk.capture_message(warning_message)
        if context is not None:
            log_op_context(context)


if __name__ == "__main__":

    def test_update_policies(context: OpExecutionContext = None) -> None:
        datahub_graph_client = DataHubGraph(
            DatahubClientConfig(
                server=settings.DATAHUB_METADATA_SERVER_URL,
                token=settings.DATAHUB_ACCESS_TOKEN,
                retry_max_times=5,
                retry_status_codes=[
                    403,
                    429,
                    500,
                    502,
                    503,
                    504,
                ],
            )
        )
        logger = get_context_with_fallback_logger(context)
        queries = ""
        for i, group_urn in enumerate(group_urns_iterator()):
            country_name = parse.unquote(
                group_urn.split("urn:li:corpGroup:")[1].split("-")[0]
            )
            logger.info(country_name)
            if is_valid_country_name(country_name):
                base_query = update_policy_base_query(group_urn=group_urn)
                queries = queries + " " + f"update{i}: {base_query}"
            else:
                warning_message = f"INVALID COUNTRY NAME: {country_name}. No Datahub Policy is created/updated for this role."
                logger.warning(warning_message)
                # log_op_context(context)
                # sentry_sdk.capture_message(warning_message)
            if i == 5:
                break

        batch_mutation_query = f"""
            mutation {{
                {queries}
            }}
            """
        try:
            logger.info("UPDATING DATAHUB POLICIES...")
            logger.info(batch_mutation_query)
            graphql_execution = datahub_graph_client.execute_graphql(
                query=batch_mutation_query
            )
            logger.info(graphql_execution)
            logger.info("DATAHUB POLICIES UPDATED SUCCESSFULLY.")
        except Exception as error:
            logger.error(error)
            log_op_context(context)
            sentry_sdk.capture_exception(error=error)

    test_update_policies()
