from datahub.ingestion.graph.filters import RemovedStatusFilter

from src.utils.datahub.graphql import get_datahub_graph_client


def list_datasets_by_filter(search_value: str) -> list:
    client = get_datahub_graph_client()
    if client is None:
        return []

    dataset_urns_iterator = client.get_urns_by_filter(
        entity_types=["dataset"], query=search_value, status=RemovedStatusFilter.ALL
    )
    return list(dataset_urns_iterator)


if __name__ == "__main__":
    qos_list = list_datasets_by_filter("qos")
    if qos_list:
        print(qos_list[0])
        client = get_datahub_graph_client()
        if client:
            client.soft_delete_entity(qos_list[0])
