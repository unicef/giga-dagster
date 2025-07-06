"""Utility functions for batch processing DataHub operations."""

from concurrent.futures import ThreadPoolExecutor, as_completed

from loguru import logger

from dagster import OpExecutionContext
from src.utils.datahub.entity import delete_entity_with_references


def delete_assertion_batch(
    context: OpExecutionContext, batch_info: tuple
) -> tuple[int, int]:
    """Delete a batch of assertions and return counts.

    Args:
        context: Dagster execution context
        batch_info: Tuple of (batch_start_index, list_of_assertion_urns)

    Returns:
        Tuple of (batch_deleted_count, batch_references_deleted_count)
    """
    batch_start, batch_urns = batch_info
    batch_deleted = 0
    batch_refs_deleted = 0

    for i, assertion_urn in enumerate(batch_urns):
        try:
            ref_count = delete_entity_with_references(context, assertion_urn)
            batch_refs_deleted += ref_count
            batch_deleted += 1
            logger.info(
                f"Batch {batch_start}-{batch_start + len(batch_urns) - 1}: "
                f"Deleted {i + 1}/{len(batch_urns)}: {assertion_urn}"
            )
        except Exception as e:
            logger.error(f"Failed to delete {assertion_urn}: {e}")

    return batch_deleted, batch_refs_deleted


def create_parallel_batches(items: list, num_parallel: int = 5) -> list[tuple]:
    """Split a list of items into parallel batches.

    Args:
        items: List of items to split into batches
        num_parallel: Number of parallel batches to create

    Returns:
        List of tuples (start_index, batch_items)
    """
    if not items:
        return []

    batch_size = len(items) // num_parallel
    if batch_size == 0:
        batch_size = 1
        num_parallel = len(items)

    batches = []
    for i in range(num_parallel):
        start_idx = i * batch_size
        end_idx = start_idx + batch_size if i < num_parallel - 1 else len(items)
        if start_idx < len(items):
            batch_items = items[start_idx:end_idx]
            batches.append((start_idx, batch_items))

    return batches


def process_batches_in_parallel(
    context: OpExecutionContext,
    batches: list[tuple],
    batch_processor_func,
    max_workers: int = None,
) -> tuple[int, int]:
    """Process multiple batches in parallel using ThreadPoolExecutor.

    Args:
        context: Dagster execution context
        batches: List of batch tuples to process
        batch_processor_func: Function to process each batch
        max_workers: Maximum number of worker threads

    Returns:
        Tuple of (total_items_processed, total_references_processed)
    """
    total_processed = 0
    total_references = 0

    if not batches:
        return total_processed, total_references

    if max_workers is None:
        max_workers = len(batches)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {
            executor.submit(batch_processor_func, context, batch): batch
            for batch in batches
        }

        for future in as_completed(future_to_batch):
            batch_info = future_to_batch[future]
            try:
                batch_processed, batch_refs = future.result()
                total_processed += batch_processed
                total_references += batch_refs
                logger.info(
                    f"Completed batch {batch_info[0]}: "
                    f"processed {batch_processed} items, {batch_refs} references"
                )
            except Exception as e:
                logger.error(f"Batch {batch_info[0]} failed: {e}")

    return total_processed, total_references
