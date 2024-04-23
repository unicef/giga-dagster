import enum
from collections.abc import Mapping
from typing import Any

from dagster import Config
from dagster._config.config_type import ConfigScalar, ConfigScalarKind


class AuthorizationTypeEnum(enum.Enum):
    BEARER_TOKEN = "BEARER_TOKEN"
    BASIC_AUTH = "BASIC_AUTH"
    API_KEY = "API_KEY"
    NONE = "NONE"


class PaginationTypeEnum(enum.Enum):
    PAGE_NUMBER = "PAGE_NUMBER"
    LIMIT_OFFSET = "LIMIT_OFFSET"
    NONE = "NONE"


class RequestMethodEnum(enum.Enum):
    POST = "POST"
    GET = "GET"


class SendQueryInEnum(enum.Enum):
    BODY = "BODY"
    QUERY_PARAMETERS = "QUERY_PARAMETERS"
    NONE = "NONE"


# class _DagsterConfigAuthorizationTypeEnum(type, ConfigScalar):
#     def __init__(cls, name, bases, dct):
#         type.__init__(cls, name, bases, dct)
#         ConfigScalar.__init__(cls, key="authorization_type", given_name="AuthorizationTypeEnum", scalar_kind=ConfigScalarKind.STRING)


# class DagsterConfigAuthorizationType(AuthorizationTypeEnum, metaclass=_DagsterConfigAuthorizationTypeEnum):

#     @classmethod
#     def _make_authorization_type_from_string(cls, value):  # Self type return
#         if isinstance(value, cls):
#             return value
#         if isinstance(value, str):
#             return cls[value]
#         raise ValueError  # your error message here

#     @classmethod
#     def __get_validators__(cls):
#         yield cls._make_authorization_type_from_string


class DagsterConfigAuthorizationType(ConfigScalar):
    key = "authorization_type"
    given_name = "AuthorizationTypeEnum"
    scalar_kind = ConfigScalarKind.STRING

    @classmethod
    def _make_authorization_type_from_string(cls, value):  # Self type return
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            return AuthorizationTypeEnum[value]
        if isinstance(value, AuthorizationTypeEnum):
            return value
        raise ValueError(f"this is wrong: {type(value)}")  # your error message here

    @classmethod
    def __get_validators__(cls):
        yield cls._make_authorization_type_from_string


class ApiConfiguration(Config):
    id: str
    api_auth_api_key: str | None
    api_auth_api_value: str | None
    api_endpoint: str
    authorization_type: DagsterConfigAuthorizationType
    basic_auth_password: str | None
    basic_auth_username: str | None
    bearer_auth_bearer_token: str | None
    data_key: str
    date_created: str
    date_last_ingested: str
    date_last_successfully_ingested: str
    date_modified: str
    enabled: bool
    error_message: str | None
    page_number_key: str | None
    page_offset_key: str | None
    page_send_query_in: str
    page_size_key: str | None
    page_starts_with: int | None
    pagination_type: str
    query_parameters: str | None
    request_body: str | None
    request_method: str
    school_id_key: str
    school_id_send_query_in: str
    size: int | None

    def _get_non_default_public_field_values_authorization_type(
        self,
    ) -> Mapping[str, Any]:
        """Returns a dictionary representation of this config object,
        ignoring any private fields, and any optional fields that are None.

        Inner fields are returned as-is in the dictionary,
        meaning any nested config objects will be returned as config objects, not dictionaries.
        """

        def map_authorization_type_field(field_key: str, value):
            field = self.__fields__.get(field_key)
            if field and issubclass(field.type_, DagsterConfigAuthorizationType):
                return value.value
            return value

        return {
            k: map_authorization_type_field(k, v)
            for k, v in super()._get_non_none_public_field_values().items()
        }

    class Config:
        orm_mode = True


# class ApiConfiguration(Config):
#     id: str
#     api_auth_api_key: str | None
#     api_auth_api_value: str | None
#     api_endpoint: str
#     authorization_type: str
#     basic_auth_password: str | None
#     basic_auth_username: str | None
#     bearer_auth_bearer_token: str | None
#     data_key: str
#     date_created: str
#     date_last_ingested: str
#     date_last_successfully_ingested: str
#     date_modified: str
#     enabled: bool
#     error_message: str | None
#     page_number_key: str | None
#     page_offset_key: str | None
#     page_send_query_in: str
#     page_size_key: str | None
#     page_starts_with: int | None
#     pagination_type: str
#     query_parameters: str | None
#     request_body: str | None
#     request_method: str
#     school_id_key: str
#     school_id_send_query_in: str
#     size: int | None

#     class Config:
#         orm_mode = True


class SchoolConnectivityConfig(ApiConfiguration):
    ingestion_frequency_minutes: int
    schema_url: str | None
    school_list_id: str
    date_key: str | None
    date_format: str | None
    send_date_in: str | None
    response_date_key: str
    response_date_format: str


class SchoolListConfig(ApiConfiguration):
    column_to_schema_mapping: dict[str, str]
    name: str
    user_email: str
    user_id: str
    school_connectivity: SchoolConnectivityConfig
