from loguru import logger

from dagster import OpExecutionContext
from src.utils.datahub.graphql import datahub_graph_client


def delete_entity_with_references(context: OpExecutionContext, urn: str) -> int:
    """Delete an entity and its references, returning the number of references deleted."""
    reference_count, _ = datahub_graph_client.delete_references_to_urn(
        urn=urn,
        dry_run=False,
    )

    if reference_count > 0:
        context.log.info(f"Deleted {reference_count} references to {urn}")

    datahub_graph_client.hard_delete_entity(urn=urn)
    logger.info(f"Hard deleted entity: {urn}")

    return reference_count
