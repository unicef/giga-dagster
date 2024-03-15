from datetime import datetime

import requests
from schemas.qos import SchoolConnectivityConfig, SchoolList
from sqlalchemy import update
from sqlalchemy.orm import Session
from src.utils.apis.common import (
    _generate_auth,
    _generate_pagination_parameters,
    _make_API_request,
)

from dagster import OpExecutionContext


def query_school_connectivity_API_data(
    context: OpExecutionContext,
    database_session: Session,
    row_data: SchoolConnectivityConfig,
    school_id: int,
) -> list:
    session = requests.Session()
    session.headers.update({"Content-Type": "application/json"})
    data = []

    auth_headers = _generate_auth(row_data)
    school_id_query_parameters = _generate_school_id_query_parameters(
        row_data, school_id
    )

    if auth_headers is not None:
        session.headers.update(auth_headers)

    if row_data["pagination_type"] is None:
        try:
            data = _make_API_request(
                context,
                session,
                row_data,
                school_id_query_parameters=school_id_query_parameters,
            )
        except Exception as e:
            update_statement = (
                update(SchoolList)
                .where(SchoolList.id == row_data["id"])
                .values({"date_last_ingested": datetime.now(), "error_message": e})
            )
            database_session.execute(update_statement)
            raise e
        else:
            update_statement = (
                update(SchoolList)
                .where(SchoolList.id == row_data["id"])
                .values(
                    {
                        "date_last_ingested": datetime.now(),
                        "date_last_succesfully_ingested": datetime.now(),
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

        while True:
            pagination_parameters = _generate_pagination_parameters(
                row_data, page, offset
            )

            try:
                run_response = data.extend(
                    _make_API_request(
                        context,
                        session,
                        row_data,
                        pagination_parameters,
                        school_id_query_parameters,
                    )
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
                error_message = f"{row_data['name']} run # {page}, offset # {total_response_count} failed: {e}"
                context.log.info(error_message)

                update_statement = (
                    update(SchoolList)
                    .where(SchoolList.id == row_data["id"])
                    .values({"date_last_ingested": datetime.now(), "error_message": e})
                )
                database_session.execute(update_statement)
                raise Exception(error_message) from e

            else:
                offset += len(run_response)
                page += 1
                context.log.info(
                    f"Next run: {row_data['name']} run # {page}, offset # {offset}"
                )

        update_statement = (
            update(SchoolList)
            .where(SchoolList.id == row_data["id"])
            .values(
                {
                    "date_last_ingested": datetime.now(),
                    "date_last_succesfully_ingested": datetime.now(),
                    "error_message": None,
                }
            )
        )
        database_session.execute(update_statement)
        return data


def _generate_school_id_query_parameters(
    row_data: SchoolConnectivityConfig, school_id: int
):
    return {row_data["school_id_key"]: school_id}
