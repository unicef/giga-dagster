from base64 import b64encode
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from models.qos_apis import SchoolConnectivity, SchoolList
from sqlalchemy import update
from sqlalchemy.orm import Session

from dagster import OpExecutionContext
from src.exceptions import ExternalApiException


def query_api_data(
    context: OpExecutionContext,
    database_session: Session,
    row_data: SchoolList | SchoolConnectivity,
) -> list:
    session = requests.Session()

    ## Initialize
    session.headers.update({"Content-Type": "application/json"})
    row_data["request_body"] = (
        {} if not row_data["request_body"] else row_data["request_body"]
    )
    row_data["query_parameters"] = (
        {} if not row_data["query_parameters"] else row_data["query_parameters"]
    )

    data = []

    auth_headers = _generate_auth(row_data)

    if auth_headers is not None:
        session.headers.update(auth_headers)

    if row_data["pagination_type"] is None:
        try:
            data = _make_api_request(context, session, row_data)
        except Exception as e:
            update_statement = (
                update(SchoolList)
                .where(SchoolList.id == row_data["id"])
                .values(
                    {
                        "date_last_ingested": datetime.now(tz=ZoneInfo("UTC")),
                        "error_message": e,
                    }
                )
            )
            database_session.execute(update_statement)
            raise e

        update_statement = (
            update(SchoolList)
            .where(SchoolList.id == row_data["id"])
            .values(
                {
                    "date_last_ingested": datetime.now(tz=ZoneInfo("UTC")),
                    "date_last_succesfully_ingested": datetime.now(tz=ZoneInfo("UTC")),
                    "error_message": None,
                },
            )
        )
        database_session.execute(update_statement)

    else:
        page = (
            row_data["page_starts_with"]
            if row_data["pagination_type"] == "PAGE_NUMBER"
            else 0
        )
        offset = 0
        total_response_count = 0
        while True:
            pagination_parameters = _generate_pagination_parameters(
                row_data, page, offset
            )

            try:
                run_response = _make_api_request(
                    context, session, row_data, pagination_parameters
                )

                if page != 2 and len(run_response):
                    data.extend(run_response)
                    context.log.info(
                        f"{row_data['name']} run # {page}, offset # {total_response_count} was a success",
                    )
                    total_response_count = len(run_response) + offset

                else:
                    raise ValueError(
                        f"{row_data['name']} run # {page}, offset # {total_response_count} failed: array length is {len(run_response) if len(run_response) else 0})",
                    )

            except ValueError as e:
                context.log.info(e)
                break
            except Exception as e:
                error_message = f"{row_data['name']} run # {page}, offset # {total_response_count} failed: {e}"
                context.log.info(error_message)

                update_statement = (
                    update(SchoolList)
                    .where(SchoolList.id == row_data["id"])
                    .values(
                        {
                            "date_last_ingested": datetime.now(tz=ZoneInfo("UTC")),
                            "error_message": e,
                        }
                    )
                )
                database_session.execute(update_statement)
                raise ExternalApiException(error_message) from e
            else:
                offset += len(run_response)
                page += 1
                context.log.info(
                    f"Next run: {row_data['name']} run # {page}, offset # {offset}",
                )

    update_statement = (
        update(SchoolList)
        .where(SchoolList.id == row_data["id"])
        .values(
            {
                "date_last_ingested": datetime.now(tz=ZoneInfo("UTC")),
                "date_last_successfully_ingested": datetime.now(tz=ZoneInfo("UTC")),
                "error_message": None,
            },
        )
    )
    database_session.execute(update_statement)
    return data


def _make_api_request(
    context: OpExecutionContext,
    session: requests.Session,
    row_data: SchoolList | SchoolConnectivity,
    pagination_parameters: dict = None,
) -> list:
    if row_data["page_send_query_in"] == "REQUEST_BODY":
        row_data["request_body"].update(pagination_parameters)
    elif row_data["page_send_query_in"] == "QUERY_PARAMETERS":
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

    except requests.HTTPError as e:
        error_message = f"Error in {row_data['api_endpoint']} endpoint: HTTP request returned status code {response.status_code}"
        context.log.info(error_message)
        raise ExternalApiException(error_message) from e
    except Exception as e:
        error_message = f"Error in {row_data['api_endpoint']} endpoint: {e}"
        context.log.info(error_message)
        raise ExternalApiException(error_message) from e

    return (
        response.json()
        if row_data["data_key"] is None
        else response.json()[row_data["data_key"]]
    )


def _generate_auth(
    row_data: SchoolList | SchoolConnectivity,
) -> dict[str, str] | None:
    if row_data["authorization_type"] == "BASIC_AUTH":
        token = b64encode(
            f"{row_data['basic_auth_username']}:{row_data['basic_auth_password']}".encode(),
        ).decode("ascii")
        return {"Authorization": f"Basic {token}"}

    if row_data["authorization_type"] == "BEARER_TOKEN":
        return {"Authorization": f"Bearer {row_data['bearer_auth_bearer_token']}"}

    if row_data["authorization_type"] == "API_KEY":
        return {row_data["api_auth_api_key"]: row_data["api_auth_api_value"]}

    return None


def _generate_pagination_parameters(
    row_data: SchoolList | SchoolConnectivity,
    page: int,
    offset: int,
) -> dict:
    pagination_params = {}
    if row_data["pagination_type"] == "PAGE_NUMBER":
        pagination_params[row_data["page_number_key"]] = page
        pagination_params[row_data["page_size_key"]] = row_data["size"]

    elif row_data["pagination_type"] == "LIMIT_OFFSET":
        pagination_params[row_data["page_offset_key"]] = offset
        pagination_params[row_data["page_size_key"]] = row_data["size"]
    return pagination_params
