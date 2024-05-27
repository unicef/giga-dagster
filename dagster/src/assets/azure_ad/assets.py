import json
import re
from http import HTTPStatus
from itertools import product

import requests
from country_converter import CountryConverter
from src.internal.graph_client import graph_credentials, graph_endpoint, graph_scopes
from unidecode import unidecode

from dagster import OpExecutionContext, asset


@asset
def create_country_dataset_roles(context: OpExecutionContext):
    coco = CountryConverter().data
    country_names = coco["name_short"].to_list()
    datasets = ["School Geolocation", "School Coverage", "School QoS"]

    roles = [
        "Admin",
        "Super",
        "Regular",
        "Developer",
        *[
            f"{country}-{dataset}"
            for country, dataset in product(country_names, datasets)
        ],
    ]
    context.log.info(f"{len(roles)=}, {roles[:10]=}")

    role_requests = []
    for role in roles:
        mail_nickname = unidecode(
            re.sub(
                r"[.,()]+",
                "",
                re.sub(r"[\s'-]+", "_", role),
            ).lower()
        )
        if mail_nickname == "admin":
            mail_nickname = "giga_admin"

        role_requests.append(
            {
                "description": role,
                "displayName": role,
                "mailEnabled": False,
                "mailNickname": mail_nickname,
                "securityEnabled": True,
                "groupTypes": [],
            }
        )

    access_token = graph_credentials.get_token(graph_scopes[0])
    headers = {"Content-Type": "application/json"}
    batch_size = 20
    count_created = 0
    count_updated = 0
    count_error = 0

    for i in range(0, len(role_requests), batch_size):
        batch_number = i // batch_size + 1
        context.log.info(f"Processing batch #{batch_number} ({batch_size=})...")

        payload = {
            "requests": [
                {
                    "id": f"{i+1}-{j+1}",
                    "method": "PATCH",
                    "url": f"""/groups(uniqueName='{body["mailNickname"]}')""",
                    "headers": {
                        **headers,
                        "Prefer": "create-if-missing",
                    },
                    "body": body,
                }
                for j, body in enumerate(role_requests[i : i + batch_size])
            ],
        }
        res = requests.post(
            url=f"{graph_endpoint}/$batch",
            headers={
                **headers,
                "Authorization": f"Bearer {access_token.token}",
            },
            json=payload,
        )
        data = res.json()

        if res.ok:
            context.log.info(f"Batch #{batch_number} POST request ok")
            for d in data["responses"]:
                if d["status"] == HTTPStatus.CREATED:
                    count_created += 1
                    context.log.info(f"Created {d['body']['displayName']}")
                elif d["status"] == HTTPStatus.NO_CONTENT:
                    count_updated += 1
                else:
                    context.log.warning(f"Unexpected status: {d['status']}\n{d}")
                    context.log.info(
                        next(
                            (p for p in payload["requests"] if p["id"] == d["id"]),
                            "No matched ID",
                        )
                    )
                    count_error += 1
        else:
            context.log.info(f"Batch #{batch_number} POST request with errors")
            count_error += 1
            context.log.info(json.dumps(data, indent=2))

    context.log.info(f"{count_created=}")
    context.log.info(f"{count_updated=}")
    context.log.warning(f"{count_error=}")
