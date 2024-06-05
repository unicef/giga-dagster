from datahub.ingestion.graph.filters import RemovedStatusFilter

from src.utils.datahub.graphql import datahub_graph_client as emitter


def list_datasets_by_filter(search_value: str) -> list:
    dataset_urns_iterator = emitter.get_urns_by_filter(
        entity_types=["dataset"], query=search_value, status=RemovedStatusFilter.ALL
    )
    return list(dataset_urns_iterator)


if __name__ == "__main__":
    qos_list = list_datasets_by_filter("qos")
    print(qos_list[0])
    emitter.soft_delete_entity(qos_list[0])
