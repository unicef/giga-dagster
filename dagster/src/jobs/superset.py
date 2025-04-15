from dagster import OpExecutionContext, job, op
import time

from ..resources.superset import get_access_token, get_saved_query, run_query

@op
def fetch_and_run_query(context: OpExecutionContext):
    access_token = get_access_token()
    if access_token:
        context.log.info("successfully authenticated with Superset")
        response = get_saved_query(access_token)
        if response.status_code == 200:
            x = response.json()
            xx = sorted(
                x.get("result"),
                key=lambda q: next(
                    (tag.get("name", "") for tag in q.get("tags", []) if tag.get("type") == 1), ""
                )
            )
        else:
            context.log.error(f"failed to fetch any saved query: {response.status_code} {response.text}")
        for query in xx:
            label = query["label"]
            context.log.info(f"running query: {label}")
            run_query(query, access_token)
            # TODO: change this to poll and wait for the result here
            if query["label"].startswith("create_"):
                time.sleep(90)
    else:
        context.log.error(
            "error in authenticating with Superset so this will be ignored now"
        )


@job
def refresh_table():
    fetch_and_run_query()
