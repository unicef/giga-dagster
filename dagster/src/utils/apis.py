from base64 import b64encode

import requests
from models.qos_apis import SchoolConnectivity, SchoolList

from dagster import OpExecutionContext


def query_API_data(
    context: OpExecutionContext, row_data: SchoolList | SchoolConnectivity
) -> list:
    session = requests.Session()
    session.headers.update({"Content-Type": "application/json"})
    data = []

    auth_headers = _generate_auth(row_data)

    if auth_headers is not None:
        session.headers.update(auth_headers)

    if row_data["pagination_type"] is None:
        return _make_API_request(context, session, row_data)

    else:
        page = (
            row_data["page_starts_with"]
            if row_data["pagination_type"] == "PAGE_NUMBER"
            else 0
        )
        offset = 0

        while True:
            pagination_parameters = _generate_pagination_parameters(
                row_data, page, offset
            )

            try:
                run_response = data.extend(
                    _make_API_request(context, session, row_data, pagination_parameters)
                )
                total_response_count = len(run_response) + offset

                if len(run_response):
                    data.extend(run_response)
                    context.log.info(
                        f"{row_data['name']} run # {page}, offset # {total_response_count} was a success"
                    )
                else:
                    raise ValueError(
                        f"{row_data['name']} run # {page}, offset # {total_response_count} failed: array length is {len(run_response)})"
                    )

            except ValueError as e:
                context.log.info(e)
                break
            except Exception as e:
                context.log.info(
                    f"{row_data['name']} run # {page}, offset # {total_response_count} failed: {e}"
                )
                raise e
            else:
                offset += len(run_response)
                page += 1
                context.log.info(
                    f"Next run: {row_data['name']} run # {page}, offset # {offset}"
                )

        return data


def _make_API_request(
    context: OpExecutionContext,
    session: requests.Session,
    row_data: SchoolList | SchoolConnectivity,
    pagination_parameters: dict = None,
) -> list:
    if row_data["send_query_in"] == "REQUEST_BODY":
        row_data["request_body"].update(pagination_parameters)
    elif row_data["send_query_in"] == "QUERY_PARAMETERS":
        row_data["query_parameters"].update(pagination_parameters)

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

    except requests.HTTPError as err:
        context.log.info(
            f"Error in {row_data["api_endpoint"]} endpoint: HTTP request returned status code"
            f" {response.status_code}"
        )
        raise err
    except Exception as e:
        context.log.info(f"Error in {row_data["api_endpoint"]} endpoint: {e}")
        raise e
    else:
        return (
            response.json()
            if row_data["data_key"] is None
            else response[row_data["data_key"]].json()
        )


def _generate_auth(
    row_data: SchoolList | SchoolConnectivity,
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
    row_data: SchoolList | SchoolConnectivity, page: int, offset: int
):
    pagination_params = {}
    if row_data.pagination_type == "PAGE_NUMBER":
        pagination_params[row_data["page_number_key"]] = page
        pagination_params[row_data["page_size_key"]] = row_data["size"]
    elif row_data.pagination_type == "LIMIT_OFFSET":
        pagination_params[row_data["page_offset_key"]] = offset
        pagination_params[row_data["page_size_key"]] = row_data["size"]
    return pagination_params
