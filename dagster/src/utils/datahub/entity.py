from loguru import logger

from dagster import OpExecutionContext
from src.utils.datahub.graphql import get_datahub_graph_client


def delete_entity_with_references(
    context: OpExecutionContext, urn: str, hard_delete: bool = False
) -> int:
    """Delete an entity and its references, returning the number of references deleted."""
    client = get_datahub_graph_client()
    if client is None:
        context.log.warning("DataHub is not configured. Skipping entity deletion.")
        return 0

    reference_count, _ = client.delete_references_to_urn(
        urn=urn,
        dry_run=False,
    )

    if reference_count > 0:
        context.log.info(f"Deleted {reference_count} references to {urn}")

    if hard_delete:
        client.hard_delete_entity(urn=urn)
    else:
        client.soft_delete_entity(urn=urn)

    logger.info(f"{'Hard' if hard_delete else 'Soft'} deleted entity: {urn}")

    return reference_count


def get_entity_count_safe(entity_type: str = "assertion", batch_size: int = 100):
    """
    Get the total count of entities using safe pagination to avoid timeouts.

    Args:
        entity_type: Type of entity to count (e.g., "assertion", "dataset")
        batch_size: Small batch size to avoid timeouts (default: 100)

    Returns:
        Total count of entities
    """
    client = get_datahub_graph_client()
    if client is None:
        print("DataHub is not configured. Skipping entity count.")
        return 0

    total_count = 0
    start = 0

    print(f"Counting {entity_type} entities with batch size {batch_size}...")

    while True:
        try:
            print(f"  Fetching batch starting at {start}")

            # Get entities for this batch with small count to avoid timeout
            entities = client.list_all_entity_urns(
                entity_type=entity_type,
                start=start,
                count=batch_size,
            )

            batch_count = len(entities)
            total_count += batch_count

            print(f"  ✅ Found {batch_count} entities (total so far: {total_count})")

            # If we got fewer entities than requested, we've reached the end
            if batch_count < batch_size:
                print(f"🎯 Reached end of {entity_type} entities")
                break

            start += batch_size

            # Safety check to prevent infinite loops
            if start > 500000:  # Reasonable safety limit
                print("⚠️ Reached safety limit, stopping count")
                break

        except Exception as e:
            print(f"❌ Error fetching batch at start={start}: {e}")
            # Try with even smaller batch size
            if batch_size > 10:
                batch_size = batch_size // 2
                print(f"🔄 Retrying with smaller batch size: {batch_size}")
                continue
            else:
                print("💥 Failed even with smallest batch size")
                break

    print(f"🎯 Final {entity_type} count: {total_count}")
    return total_count
