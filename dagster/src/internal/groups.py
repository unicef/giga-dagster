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
                "mailNickname",
                "displayName",
                "userPrincipalName",
                "accountEnabled",
                "externalUserState",
                "givenName",
                "surname",
                "otherMails",
                "identities",
            ],
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
                        u = GraphUser.from_orm(user)
                        if u.mail is None:
                            u.mail = cls.get_user_email_from_api(u)
                        members[user.id] = u

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

                return users_list
            except ODataError as err:
                raise Exception(err.error.message) from err

        return []
