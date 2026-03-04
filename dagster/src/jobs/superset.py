import os
import time

import requests

from dagster import OpExecutionContext, job, op

from ..resources.superset import (
    fetch_saved_query,
    get_access_token,
    refresh_access_token,
    run_query,
)


@op
def post_query_durations_to_slack(context: OpExecutionContext, results):
    slack_webhook = os.getenv("SLACK_WORKFLOW_WEBHOOK")
    if not slack_webhook:
        context.log.warning("No SLACK_WORKFLOW_WEBHOOK configured")
        return

    lines = []
    for r in results:
        duration = round(r["duration"], 2)
        status = r.get("status_code", "ERR")
        lines.append(f"{r['title']} - {duration}s | status {status}")

    deployment_environment = os.getenv("DEPLOY_ENV")
    if deployment_environment == "stg":
        payload = {"text": "*Refresh Table - Staging*\n" + "\n".join(lines)}
    elif deployment_environment == "prd":
        payload = {"text": "*Refresh Table - Production*\n" + "\n".join(lines)}
    resp = requests.post(slack_webhook, json=payload)
    context.log.info(f"posted to Slack ({resp.status_code})")


@op
def fetch_and_run_query(context: OpExecutionContext):
    results = []
    try:
        access_token = None
        refresh_token = None
        auth_data = get_access_token()

        access_token = auth_data.get("access_token")
        refresh_token = auth_data.get("refresh_token")
        if access_token:
            context.log.info("successfully authenticated with Superset")
        else:
            context.log.info("unable to authenticate with Superset")
            raise
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
            context.log.info(f"status code: {x.get('status_code')}")
            context.log.info(f"response: {x.get('response_text')}")
            time.sleep(5)
            x = run_query(query_to_create, access_token)
            context.log.info(f"status code: {x.get('status_code')}")
            context.log.info(f"response: {x.get('response_text')}")
            time.sleep(90)

            x["title"] = table["Title"]
            results.append(x)

            if x.get("status_code") != 200:
                return results
            response = refresh_access_token(refresh_token)
            if response.status_code == 200:
                auth_data = response.json()
                access_token = auth_data.get("access_token")
            else:
                context.log.error(
                    "error in authenticating with Superset so unable to run the query now"
                )
    except Exception as e:
        context.log.error(e)
        return results
    return results


@job
def refresh_table():
    results = fetch_and_run_query()
    post_query_durations_to_slack(results)
