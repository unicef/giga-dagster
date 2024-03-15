from base64 import b64encode

import requests
from src.schemas.qos import SchoolConnectivityConfig, SchoolListConfig

from dagster import OpExecutionContext


def _make_API_request(
    context: OpExecutionContext,
    session: requests.Session,
    row_data: SchoolListConfig | SchoolConnectivityConfig,
    pagination_parameters: dict = None,
    school_id_query_parameters: dict = None,
) -> list:
    _update_parameters(row_data, pagination_parameters, "page_send_query_in")
    _update_parameters(row_data, school_id_query_parameters, "school_id_send_query_in")

    try:
        if row_data["request_method"] == "GET":
            response = session.get(
                row_data["api_endpoint"],
                params=row_data["query_parameters"],
            )
        elif row_data["request_method"] == "POST":
            response = session.post(
                row_data["api_endpoint"],
                params=row_data["query_parameters"],
                data=row_data["request_body"],
            )

        response.raise_for_status()

    except requests.HTTPError as e:
        error_message = f"Error in {row_data['api_endpoint']} endpoint: HTTP request returned status code {response.status_code}"
        context.log.info(error_message)
        raise Exception(error_message) from e
    except Exception as e:
        error_message = f"Error in {row_data['api_endpoint']} endpoint: {e}"
        context.log.info(error_message)
        raise Exception(error_message) from e
    else:
        return (
            response.json()
            if row_data["data_key"] is None
            else response[row_data["data_key"]].json()
        )


def _generate_auth(
    row_data: SchoolListConfig | SchoolConnectivityConfig,
):
    if row_data["authorization_type"] == "BASIC_AUTH":
        token = b64encode(
            f"{row_data['basic_auth_username']}:{row_data['basic_auth_password']}".encode()
        ).decode("ascii")
        return {"Authorization": f"Basic {token}"}
    elif row_data["authorization_type"] == "BEARER_TOKEN":
        return {"Authorization": f"Bearer {row_data['bearer_auth_bearer_token']}"}
    elif row_data["authorization_type"] == "API_KEY":
        return {row_data["api_auth_api_key"]: row_data["api_auth_api_value"]}


def _generate_pagination_parameters(
    row_data: SchoolListConfig | SchoolConnectivityConfig, page: int, offset: int
):
    pagination_params = {}
    if row_data.pagination_type == "PAGE_NUMBER":
        pagination_params[row_data["page_number_key"]] = page
        pagination_params[row_data["page_size_key"]] = row_data["size"]
    elif row_data.pagination_type == "LIMIT_OFFSET":
        pagination_params[row_data["page_offset_key"]] = offset
        pagination_params[row_data["page_size_key"]] = row_data["size"]
    return pagination_params


def _update_parameters(
    row_data: SchoolListConfig | SchoolConnectivityConfig,
    parameters: dict,
    parameter_send_key: str,
) -> None:
    if parameters:
        if row_data[parameter_send_key] == "REQUEST_BODY":
            row_data["request_body"].update(parameters)
        elif row_data[parameter_send_key] == "QUERY_PARAMETERS":
            row_data["query_parameters"].update(parameters)
