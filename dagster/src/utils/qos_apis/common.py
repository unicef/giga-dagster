import json
from base64 import b64encode

import requests
from models.qos_apis import SchoolConnectivity, SchoolList
from src.exceptions import ExternalApiException

from dagster import OpExecutionContext


def _make_api_request(
    context: OpExecutionContext,
    session: requests.Session,
    row_data: SchoolList | SchoolConnectivity,
) -> list:
    context.log.info(
        f"Making request to {row_data['api_endpoint']}, query: {row_data['query_parameters']}, body: {row_data['request_body']}"
    )
    response = None
    try:
        if row_data["request_method"] == "GET":
            response = session.get(
                row_data["api_endpoint"],
                params=row_data["query_parameters"],
            )
            response.raise_for_status()
        elif row_data["request_method"] == "POST":
            response = session.post(
                row_data["api_endpoint"],
                params=row_data["query_parameters"],
                data=json.dumps(row_data["request_body"]),
            )
            response.raise_for_status()

    except requests.HTTPError as e:
        error_message = f"Error in {row_data['api_endpoint']} endpoint: HTTP request returned status code {response.status_code}"
        context.log.info(error_message)
        raise ExternalApiException(error_message) from e
    except Exception as e:
        error_message = f"Error in {row_data['api_endpoint']} endpoint: {e}"
        context.log.info(error_message)
        raise ExternalApiException(error_message) from e
    else:
        response_data = (
            response.json()
            if not row_data["data_key"]
            else response.json()[row_data["data_key"]]
        )
        if isinstance(response_data, dict) and len(response_data) == 1:
            nested_value = list(response_data.values())[0]
            if isinstance(nested_value, dict):
                response_data = _flatten_nested_dict(nested_value)

        response_data = (
            response_data if isinstance(response_data, list) else [response_data]
        )
        context.log.info(f"Data: {response_data}")
        return response_data


def _generate_auth_parameters(
    row_data: SchoolList | SchoolConnectivity,
) -> dict[str, str] | None:
    if row_data["authorization_type"] == "BASIC_AUTH":
        token = b64encode(
            f"{row_data['basic_auth_username']}:{row_data['basic_auth_password']}".encode(),
        ).decode("ascii")
        return {"Authorization": f"Basic {token}"}

    elif row_data["authorization_type"] == "BEARER_TOKEN":
        return {"Authorization": f"Bearer {row_data['bearer_auth_bearer_token']}"}

    elif row_data["authorization_type"] == "API_KEY":
        return {row_data["api_auth_api_key"]: row_data["api_auth_api_value"]}

    return None


def _generate_pagination_parameters(
    row_data: SchoolList | SchoolConnectivity,
    page: int,
    offset: int,
) -> None:
    if row_data["pagination_type"] != "NONE":
        pagination_parameters = {}
        if row_data["pagination_type"] == "PAGE_NUMBER":
            pagination_parameters[row_data["page_number_key"]] = page
            pagination_parameters[row_data["page_size_key"]] = row_data["size"]

        elif row_data["pagination_type"] == "LIMIT_OFFSET":
            pagination_parameters[row_data["page_offset_key"]] = offset
            pagination_parameters[row_data["page_size_key"]] = row_data["size"]

        _update_parameters(row_data, pagination_parameters, "page_send_query_in")


def _update_parameters(
    row_data: SchoolList | SchoolConnectivity,
    parameters: dict,
    parameter_send_key: str,
) -> None:
    if parameters:
        if row_data[parameter_send_key] == "BODY":
            row_data["request_body"].update(parameters)
        elif row_data[parameter_send_key] == "QUERY_PARAMETERS":
            row_data["query_parameters"].update(parameters)


def _flatten_nested_dict(nested_dict):
    flat_dict = {}
    for key, value in nested_dict.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                flat_dict[sub_key] = sub_value
        else:
            flat_dict[key] = value
    return flat_dict
