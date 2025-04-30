from dagster import OpExecutionContext, job, op
import time
from ..resources.superset import get_access_token, fetch_saved_query, run_query

@op
def fetch_and_run_query(context: OpExecutionContext):
    access_token = get_access_token()
    if access_token:
        context.log.info("successfully authenticated with Superset")
        response = fetch_saved_query()
        for table in response:
            query_to_delete = {
                "label": "Query to Delete",
                "sql": table["Query to Delete"]
            }
            query_to_create = {
                "label": "Query to Create",
                "sql": table["Query to Create"]
            }
            context.log.info(f"running query: {table['title']}")
            run_query(query_to_delete, access_token)
            time.sleep(5)
            # TODO: change this to poll and wait for the result here
            run_query(query_to_create, access_token)
            time.sleep(90)
    else:
        context.log.error(
            "error in authenticating with Superset so this will be ignored now"
        )


@job
def refresh_table():
    fetch_and_run_query()
