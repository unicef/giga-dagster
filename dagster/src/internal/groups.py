import country_converter as coco
import requests
from loguru import logger
from msgraph import GraphServiceClient
from msgraph.generated.groups.groups_request_builder import (
    GroupsRequestBuilder,
)
from msgraph.generated.models.o_data_errors.o_data_error import ODataError
from msgraph.generated.users.users_request_builder import UsersRequestBuilder

from azure.identity import ClientSecretCredential
from src.schemas.user import GraphUser
from src.settings import settings
from src.utils.string import to_snake_case

graph_scopes = ["https://graph.microsoft.com/.default"]
graph_credentials = ClientSecretCredential(
    tenant_id=settings.AAD_AZURE_TENANT_ID,
    client_id=settings.AAD_AZURE_CLIENT_ID,
    client_secret=settings.AAD_AZURE_CLIENT_SECRET,
)
graph_client = GraphServiceClient(credentials=graph_credentials, scopes=graph_scopes)


class GroupsApi:
    get_group_query_parameters = (
        GroupsRequestBuilder.GroupsRequestBuilderGetQueryParameters(
            select=["id", "description", "displayName"],
            filter="securityEnabled eq true",
            top=999,
        )
    )
    group_request_config = (
        GroupsRequestBuilder.GroupsRequestBuilderGetRequestConfiguration(
            query_parameters=get_group_query_parameters,
        )
    )

    get_user_query_select_fields = [
        "id",
        "mail",
        "mailNickname",
        "displayName",
        "userPrincipalName",
        "accountEnabled",
        "externalUserState",
        "givenName",
        "surname",
        "otherMails",
        "identities",
    ]
    get_user_query_parameters = (
        UsersRequestBuilder.UsersRequestBuilderGetQueryParameters(
            select=get_user_query_select_fields,
            orderby=["displayName", "mail", "userPrincipalName"],
            count=True,
            top=999,
        )
    )
    user_request_config = (
        UsersRequestBuilder.UsersRequestBuilderGetRequestConfiguration(
            query_parameters=get_user_query_parameters,
        )
    )

    @staticmethod
    def get_user_email_from_api(user: GraphUser) -> str | None:
        if user.mail is not None:
            return user.mail

        if "#EXT#" in user.user_principal_name:
            if user.other_mails is not None and len(user.other_mails) > 0:
                return user.other_mails[0]
            else:
                escaped_email_part = user.user_principal_name.split("#EXT#")[0]
                return "@".join(escaped_email_part.rsplit("_", 1))

        if user.identities is not None and len(user.identities) > 0:
            email_identity = next(
                (id for id in user.identities if id.sign_in_type == "emailAddress"),
                None,
            )
            if email_identity is not None:
                return email_identity.issuer_assigned_id

    @classmethod
    def batch_get_user_details(cls, users: list[GraphUser]) -> list[GraphUser]:
        access_token = graph_credentials.get_token(
            "https://graph.microsoft.com/.default"
        )
        graph_api_endpoint = "https://graph.microsoft.com/v1.0"
        batch_size = 20
        headers = {
            "Authorization": f"Bearer {access_token[0]}",
            "Content-Type": "application/json",
        }
        select_params = ",".join(cls.get_user_query_select_fields)

        all_responses = []

        for i in range(0, len(users), batch_size):
            payload = [
                {
                    "id": f"{i}-{j}",
                    "method": "GET",
                    "url": f"/users/{user.id}?$select={select_params}",
                }
                for j, user in enumerate(users[i : i + batch_size])
            ]

            response = requests.post(
                url=f"{graph_api_endpoint}/$batch",
                headers=headers,
                json={"requests": payload},
            )
            if response.ok:
                data = response.json()
                if data.get("responses") is not None:
                    all_responses.extend(data["responses"])
            else:
                logger.error(response.json())

        users_details = []
        for r in all_responses:
            if r.get("body") is not None:
                transformed_body = to_snake_case(r["body"])
                graph_user = GraphUser(**transformed_body)
                if graph_user.mail is None:
                    graph_user.mail = cls.get_user_email_from_api(graph_user)
                users_details.append(graph_user)

        return users_details

    @classmethod
    async def list_country_members(cls, country_code: str) -> dict[str, GraphUser]:
        full_country_name = coco.convert(names=[country_code], to="name_short")
        members = {}

        try:
            groups = await graph_client.groups.get(
                request_configuration=cls.group_request_config
            )
            filtered_groups = [
                item.id
                for item in groups.value
                if item.display_name.rsplit("-", 1)[0] == full_country_name
            ]

        except ODataError as err:
            raise Exception(err.error.message) from err

        if filtered_groups:
            for country_id in filtered_groups:
                try:
                    updated_request_configuration = cls.user_request_config.headers.add(
                        "ConsistencyLevel", "eventual"
                    )
                    users = await graph_client.groups.by_group_id(
                        country_id
                    ).members.get(request_configuration=updated_request_configuration)

                    for user in users.value:
                        members[user.id] = GraphUser.from_orm(user)

                except ODataError as err:
                    raise Exception(err.error.message) from err

        detailed_members = cls.batch_get_user_details(list(members.values()))
        for member in detailed_members:
            if member.mail is None:
                member.mail = cls.get_user_email_from_api(member)
        return {str(d.id): d for d in detailed_members}

    @classmethod
    async def list_group_members(cls, group_name: str) -> list[GraphUser]:
        try:
            all_groups = await graph_client.groups.get(
                request_configuration=cls.group_request_config
            )
            group = next(
                (
                    item.id
                    for item in all_groups.value
                    if item.display_name == group_name
                ),
                None,
            )

        except ODataError as err:
            raise Exception(err.error.message) from err

        users_list = []

        if group is not None:
            try:
                updated_request_configuration = cls.user_request_config.headers.add(
                    "ConsistencyLevel", "eventual"
                )
                users = await graph_client.groups.by_group_id(group).members.get(
                    request_configuration=updated_request_configuration
                )
                for user in users.value:
                    u = GraphUser.from_orm(user)
                    if u.mail is None:
                        u.mail = cls.get_user_email_from_api(u)
                    users_list.append(u)

                return cls.batch_get_user_details(users_list)
            except ODataError as err:
                raise Exception(err.error.message) from err

        return []
