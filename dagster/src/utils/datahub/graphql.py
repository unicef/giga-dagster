import sentry_sdk
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

from dagster import OpExecutionContext
from src.settings import settings
from src.utils.logger import get_context_with_fallback_logger
from src.utils.sentry import log_op_context

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


def execute_batch_mutation(queries: str, context: OpExecutionContext = None):
    logger = get_context_with_fallback_logger(context)

    batch_mutation_query = f"""
        mutation {{
            {queries}
        }}
        """
    try:
        logger.info("EXECUTING DATAHUB GRAPHQL BATCH MUTATIONS...")
        logger.info(batch_mutation_query)
        graphql_execution = datahub_graph_client.execute_graphql(
            query=batch_mutation_query
        )
        logger.info(graphql_execution)
        logger.info("DATAHUB BATCH MUTATIONS SUCCESSFUL.")
    except Exception as error:
        logger.error(error)
        if context is not None:
            log_op_context(context)
        sentry_sdk.capture_exception(error=error)
