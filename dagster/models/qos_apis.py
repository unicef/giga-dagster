import enum

from sqlalchemy import JSON, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from models.base_database import BaseModel


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


# class ApiConfiguration(BaseModel):
#     __abstract__ = True

#     api_auth_api_key: Mapped[str] = mapped_column(nullable=True)
#     api_auth_api_value: Mapped[str] = mapped_column(nullable=True)
#     api_endpoint: Mapped[str] = mapped_column(nullable=True)
#     authorization_type: Mapped[AuthorizationTypeEnum] = mapped_column(
#         Enum(AuthorizationTypeEnum),
#         default=AuthorizationTypeEnum.NONE,
#         nullable=True,
#     )
#     basic_auth_password: Mapped[str] = mapped_column(nullable=True)
#     basic_auth_username: Mapped[str] = mapped_column(nullable=True)
#     bearer_auth_bearer_token: Mapped[str] = mapped_column(nullable=True)

#     data_key: Mapped[str] = mapped_column(nullable=True)
#     date_created: Mapped[str] = mapped_column(
#         DateTime(timezone=True),
#         server_default=func.now(),
#     )
#     date_modified: Mapped[str] = mapped_column(
#         DateTime(timezone=True),
#         server_default=func.now(),
#         onupdate=func.now(),
#     )
#     date_last_successfully_ingested: Mapped[str] = mapped_column(
#         DateTime(timezone=True),
#         server_default=func.now(),
#         onupdate=func.now(),
#     )
#     date_last_ingested: Mapped[str] = mapped_column(
#         DateTime(timezone=True),
#         server_default=func.now(),
#         onupdate=func.now(),
#     )

#     enabled: Mapped[bool] = mapped_column(default=True)
#     error_message: Mapped[str] = mapped_column(nullable=True)

#     page_number_key: Mapped[str] = mapped_column(nullable=True)
#     page_offset_key: Mapped[str] = mapped_column(nullable=True)
#     page_size_key: Mapped[str] = mapped_column(nullable=True)
#     page_starts_with: Mapped[int] = mapped_column(nullable=True)
#     pagination_type: Mapped[PaginationTypeEnum] = mapped_column(
#         Enum(PaginationTypeEnum),
#         default=PaginationTypeEnum.NONE,
#         nullable=True,
#     )

#     query_parameters: Mapped[dict] = mapped_column(JSON, nullable=True)
#     request_body: Mapped[dict] = mapped_column(JSON, nullable=True)

#     request_method: Mapped[RequestMethodEnum] = mapped_column(
#         Enum(RequestMethodEnum),
#         default=RequestMethodEnum.GET,
#         nullable=True,
#     )
#     school_id_key: Mapped[str] = mapped_column(nullable=True)
#     page_send_query_in: Mapped[SendQueryInEnum] = (
#         mapped_column(
#             Enum(SendQueryInEnum),
#             default=SendQueryInEnum.NONE,
#             nullable=True,
#         ),
#     )
#     school_id_send_query_in: Mapped[SendQueryInEnum] = (
#         mapped_column(
#             Enum(SendQueryInEnum),
#             default=SendQueryInEnum.NONE,
#             nullable=True,
#         ),
#     )
#     size: Mapped[int] = mapped_column(nullable=True)


class ApiConfiguration(BaseModel):
    __abstract__ = True

    api_auth_api_key: Mapped[str] = mapped_column(nullable=True)
    api_auth_api_value: Mapped[str] = mapped_column(nullable=True)
    api_endpoint: Mapped[str] = mapped_column(nullable=True)
    authorization_type: Mapped[str] = mapped_column(nullable=True)
    basic_auth_password: Mapped[str] = mapped_column(nullable=True)
    basic_auth_username: Mapped[str] = mapped_column(nullable=True)
    bearer_auth_bearer_token: Mapped[str] = mapped_column(nullable=True)

    data_key: Mapped[str] = mapped_column(nullable=True)
    date_created: Mapped[str] = mapped_column(nullable=True)
    date_modified: Mapped[str] = mapped_column(nullable=True)
    date_last_successfully_ingested: Mapped[str] = mapped_column(nullable=True)
    date_last_ingested: Mapped[str] = mapped_column(nullable=True)

    enabled: Mapped[bool] = mapped_column(default=True)
    error_message: Mapped[str] = mapped_column(nullable=True)

    page_number_key: Mapped[str] = mapped_column(nullable=True)
    page_offset_key: Mapped[str] = mapped_column(nullable=True)
    page_size_key: Mapped[str] = mapped_column(nullable=True)
    page_starts_with: Mapped[int] = mapped_column(nullable=True)
    pagination_type: Mapped[str] = mapped_column(nullable=True)

    query_parameters: Mapped[dict] = mapped_column(JSON, nullable=True)
    request_body: Mapped[dict] = mapped_column(JSON, nullable=True)

    request_method: Mapped[str] = mapped_column(nullable=True)
    school_id_key: Mapped[str] = mapped_column(nullable=True)
    page_send_query_in: Mapped[str] = mapped_column(nullable=True)
    school_id_send_query_in: Mapped[str] = mapped_column(nullable=True)
    size: Mapped[int] = mapped_column(nullable=True)


class SchoolList(ApiConfiguration):
    __tablename__ = "qos_school_list"

    column_to_schema_mapping: Mapped[dict] = mapped_column(
        JSON,
        nullable=True,
        server_default="",
    )
    name: Mapped[str] = mapped_column(nullable=True, server_default="")
    user_email: Mapped[str] = mapped_column(String(), nullable=True)
    user_id: Mapped[str] = mapped_column(nullable=True)

    school_connectivity: Mapped["SchoolConnectivity"] = relationship(
        "SchoolConnectivity",
        back_populates="school_list",
    )


class SchoolConnectivity(ApiConfiguration):
    __tablename__ = "qos_school_connectivity"

    ingestion_frequency_minutes: Mapped[int] = mapped_column()
    schema_url: Mapped[str] = mapped_column()

    school_list_id: Mapped[str] = mapped_column(ForeignKey("qos_school_list.id"))
    school_list: Mapped["SchoolList"] = relationship(
        "SchoolList", back_populates="school_connectivity"
    )

    date_key: Mapped[str] = mapped_column(nullable=True, default=None)
    date_format: Mapped[str] = mapped_column(nullable=True, default=None)
    send_date_in: Mapped[str] = mapped_column(nullable=True, default=None)
    response_date_key: Mapped[str] = mapped_column(nullable=True, default="")
    response_date_format: Mapped[str] = mapped_column(nullable=True, default="%Y-%m-%d")
