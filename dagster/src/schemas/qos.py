import enum
from datetime import datetime

from dagster import Config


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


class ApiConfiguration(Config):
    id: str
    api_auth_api_key: str | None
    api_auth_api_value: str | None
    api_endpoint: str
    authorization_type: AuthorizationTypeEnum
    basic_auth_password: str | None
    basic_auth_username: str | None
    bearer_auth_bearer_token: str | None
    data_key: str | None
    date_created: datetime
    date_last_ingested: datetime
    date_last_successfully_ingested: datetime
    date_modified: datetime
    enabled: bool
    error_message: str | None
    page_number_key: str | None
    page_offset_key: str | None
    page_send_query_in: str
    page_size_key: str | None
    page_starts_with: int | None
    pagination_type: PaginationTypeEnum
    query_parameters: str | None
    request_body: str | None
    request_method: RequestMethodEnum
    school_id_key: str | None
    size: int | None

    class Config:
        orm_mode = True


class SchoolConnectivityConfig(ApiConfiguration):
    ingestion_frequency_minutes: int
    schema_url: str | None
    school_list_id: str
    date_key: str | None
    date_format: str | None
    send_date_in: str | None
    response_date_key: str
    response_date_format: str
    school_id_send_query_in: SendQueryInEnum
    has_school_id_giga: bool
    school_id_giga_govt_key: str


class SchoolListConfig(ApiConfiguration):
    column_to_schema_mapping: dict[str, str]
    name: str
    user_email: str
    user_id: str
    country: str
