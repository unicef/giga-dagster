from dagster import OpExecutionContext, job, op

from ..resources.superset import get_access_token, get_saved_query, run_query


@op
def fetch_and_run_query(context: OpExecutionContext):
    access_token = get_access_token()

    if access_token:
        context.log.info("successfully authenticated with Superset")
        response = get_saved_query(access_token)

        if response.status_code == 200:
            x = response.json()

            xx = {}
            yy = {}

            for query in x.get("result"):
                label = query["label"]
                context.log.info(f"found {label}")
                if label.startswith("create_"):
                    table_name = label.split("create_")[1]
                    xx[table_name] = query
                elif label.startswith("delete_"):
                    table_name = label.split("delete_")[1]
                    yy[table_name] = query

        else:
            context.log.error(
                f"failed to fetch any saved query: {response.status_code} {response.text}"
            )

        for table_name in xx:
            if table_name in yy:
                context.log.info(f"deleting table: {table_name}")
                query_delete = yy[table_name]
                run_query(query_delete, access_token)

            query_create = xx[table_name]
            context.log.info(f"creating table: {table_name}")
            run_query(query_create, access_token)
    else:
        context.log.error(
            "error in authenticating with Superset so this will be ignored now"
        )


@job
def refresh_table():
    fetch_and_run_query()
