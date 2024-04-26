import country_converter as coco
from msgraph import GraphServiceClient
from msgraph.generated.groups.groups_request_builder import (
    GroupsRequestBuilder,
)
from msgraph.generated.models.o_data_errors.o_data_error import ODataError
from msgraph.generated.users.users_request_builder import UsersRequestBuilder

from azure.identity import ClientSecretCredential
from src.schemas.user import GraphUser
from src.settings import settings

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
    get_user_query_parameters = (
        UsersRequestBuilder.UsersRequestBuilderGetQueryParameters(
            select=[
                "id",
                "mail",
                "displayName",
                "userPrincipalName",
                "accountEnabled",
                "externalUserState",
            ],
            orderby=["displayName", "mail", "userPrincipalName"],
            count=True,
        )
    )
    user_request_config = (
        UsersRequestBuilder.UsersRequestBuilderGetRequestConfiguration(
            query_parameters=get_user_query_parameters,
        )
    )

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
                        members[user.id] = user

                except ODataError as err:
                    raise Exception(err.error.message) from err

        return members

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

        if group is not None:
            try:
                updated_request_configuration = cls.user_request_config.headers.add(
                    "ConsistencyLevel", "eventual"
                )
                users = await graph_client.groups.by_group_id(group).members.get(
                    request_configuration=updated_request_configuration
                )
                return [GraphUser.from_orm(u) for u in users.value]
            except ODataError as err:
                raise Exception(err.error.message) from err

        return []
