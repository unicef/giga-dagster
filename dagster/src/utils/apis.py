import requests

from dagster import InitResourceContext


def _get_many(
    context: InitResourceContext,
    base_api_url: str,
    endpoint: str,
    api_key: str,
    **kwargs,
) -> list:
    session = requests.Session()
    session.headers.update({"authtoken": api_key, "Content-Type": "application/json"})

    try:
        response = session.get(
            f"{base_api_url}/{endpoint}",
            params={"modelType": kwargs.get("modelType", "full")},
        )
        response.raise_for_status()

    except requests.HTTPError:
        context.log.info(
            f"Error in {endpoint} endpoint: HTTP request returned status code"
            f" {response.status_code}"
        )
        raise
    except Exception as e:
        context.log.info(f"Error in {endpoint} endpoint: {e}")
        raise
    else:
        return response.json()
