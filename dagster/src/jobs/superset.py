import time

from dagster import OpExecutionContext, job, op

from ..resources.superset import fetch_saved_query, get_access_token, refresh_access_token, run_query


@op
def fetch_and_run_query(context: OpExecutionContext):
    access_token = None
    refresh_token = None
    auth_data = get_access_token()

    if auth_data:
        access_token = auth_data.get("access_token")
        refresh_token = auth_data.get("refresh_token")

    if access_token:
        context.log.info("successfully authenticated with Superset")
        response = fetch_saved_query()
        for table in response:
            query_to_delete = {
                "label": "Query to Delete",
                "sql": table["Query to Delete"],
            }
            query_to_create = {
                "label": "Query to Create",
                "sql": table["Query to Create"],
            }
            context.log.info(f"running query: {table['Title']}")
            x = run_query(query_to_delete, access_token)
            context.log.info(f"status code: {x.status_code}")
            context.log.info(f"response: {x.text}")
            time.sleep(5)
            # TODO: change this to poll and wait for the result here
            x = run_query(query_to_create, access_token)
            context.log.info(f"status code: {x.status_code}")
            context.log.info(f"response: {x.text}")
            time.sleep(90)
            response = refresh_access_token(refresh_token)
            if response.status_code == 200:
                auth_data = response.json()
                access_token = auth_data.get("access_token")
            else:
                context.log.error(
                    "error in authenticating with Superset so unable to run the query now"
                )
    else:
        context.log.error(
            "error in authenticating with Superset so this will be ignored now"
        )


@job
def refresh_table():
    fetch_and_run_query()