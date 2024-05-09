from datetime import datetime

from models.qos_apis import (
    AuthorizationTypeEnum,
    PaginationTypeEnum,
    RequestMethodEnum,
    SendParametersInEnum,
)

from dagster import Config


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
    page_send_query_in: SendParametersInEnum
    page_size_key: str | None
    page_starts_with: int | None
    pagination_type: PaginationTypeEnum
    query_parameters: str | None
    request_body: str | None
    request_method: RequestMethodEnum
    school_id_key: str
    size: int | None

    class Config:
        orm_mode = True


class SchoolListConfig(ApiConfiguration):
    column_to_schema_mapping: dict[str, str]
    name: str
    user_email: str
    user_id: str


class SchoolConnectivityConfig(ApiConfiguration):
    ingestion_frequency: str
    schema_url: str | None
    school_list_id: str
    date_key: str | None
    date_format: str | None
    send_date_in: SendParametersInEnum
    response_date_key: str
    response_date_format: str
    school_id_send_query_in: SendParametersInEnum
    school_list: SchoolListConfig
    has_school_id_giga: bool
    school_id_giga_govt_key: str
