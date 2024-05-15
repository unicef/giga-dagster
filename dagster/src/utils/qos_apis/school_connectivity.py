from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from models.qos_apis import SchoolConnectivity
from sqlalchemy import update
from sqlalchemy.orm import Session
from src.exceptions import ExternalApiException
from src.utils.qos_apis.common import (
    _generate_auth_parameters,
    _generate_pagination_parameters,
    _make_api_request,
    _update_parameters,
)

from dagster import OpExecutionContext


def query_school_connectivity_data(
    context: OpExecutionContext,
    database_session: Session,
    row_data: SchoolConnectivity,
    school_id: int = None,
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

    auth_headers = _generate_auth_parameters(row_data)
    _generate_school_id_query_parameters(row_data, school_id)
    _generate_date_query_parameters(row_data)

    context.log.info(
        f"schoolidq: {row_data['query_parameters']}, {row_data['request_body']}"
    )

    if auth_headers is not None:
        session.headers.update(auth_headers)

    if row_data["pagination_type"] == "NONE":
        try:
            data = _make_api_request(
                context,
                session,
                row_data,
            )
        except Exception as e:
            update_statement = (
                update(SchoolConnectivity)
                .where(SchoolConnectivity.id == row_data["id"])
                .values(
                    {
                        "date_last_ingested": datetime.now(tz=ZoneInfo("UTC")),
                        "error_message": e,
                    }
                )
            )
            database_session.execute(update_statement)
            raise e
        else:
            update_statement = (
                update(SchoolConnectivity)
                .where(SchoolConnectivity.id == row_data["id"])
                .values(
                    {
                        "date_last_ingested": datetime.now(tz=ZoneInfo("UTC")),
                        "date_last_successfully_ingested": datetime.now(
                            tz=ZoneInfo("UTC")
                        ),
                        "error_message": None,
                    }
                )
            )
            database_session.execute(update_statement)

        return data

    else:
        page = (
            row_data["page_starts_with"]
            if row_data["pagination_type"] == "PAGE_NUMBER"
            else 0
        )
        offset = 0
        total_response_count = 0

        while True:
            _generate_pagination_parameters(row_data, page, offset)

            try:
                run_response = _make_api_request(
                    context,
                    session,
                    row_data,
                )

                if len(run_response):
                    data.extend(run_response)
                    context.log.info(
                        f"{row_data['school_list']['name']} run # {page}, offset # {total_response_count} was a success"
                    )
                    total_response_count = len(run_response) + offset

                else:
                    raise ValueError(
                        f"{row_data['school_list']['name']} run # {page}, offset # {total_response_count} failed: array length is {len(run_response)})"
                    )

            except ValueError as e:
                context.log.info(e)
                break
            except Exception as e:
                error_message = f"{row_data['school_list']['name']} run # {page}, offset # {total_response_count} failed: {e}"
                context.log.info(error_message)

                update_statement = (
                    update(SchoolConnectivity)
                    .where(SchoolConnectivity.id == row_data["id"])
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
                    f"Next run: {row_data['school_list']['name']} run # {page}, offset # {offset}"
                )

    update_statement = (
        update(SchoolConnectivity)
        .where(SchoolConnectivity.id == row_data["id"])
        .values(
            {
                "date_last_ingested": datetime.now(tz=ZoneInfo("UTC")),
                "date_last_successfully_ingested": datetime.now(tz=ZoneInfo("UTC")),
                "error_message": None,
            }
        )
    )
    database_session.execute(update_statement)
    return data


def _generate_school_id_query_parameters(
    row_data: SchoolConnectivity, school_id: int
) -> None:
    if (
        row_data["school_id_key"]
        and school_id
        and row_data["school_id_send_query_in"] != "NONE"
    ):
        school_id_parameters = {}
        school_id_parameters[row_data["school_id_key"]] = school_id

        _update_parameters(row_data, school_id_parameters, "school_id_send_query_in")


def _generate_date_query_parameters(row_data: SchoolConnectivity) -> None:
    if (
        row_data["date_key"]
        and row_data["date_format"]
        and row_data["date_send_query_in"] != "NONE"
    ):
        date_parameters = {}
        date_parameters[row_data["date_key"]] = datetime.now().strftime(
            row_data["date_format"]
        )

        _update_parameters(row_data, date_parameters, "date_send_query_in")
