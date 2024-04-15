from datahub.emitter import mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataplatform import DataPlatformInfo
from datahub.metadata.schema_classes import (
    PlatformTypeClass,
)
from pydantic import AnyUrl

from src.settings import settings


def add_platform_metadata(
    platform: str,
    display_name: str,
    logo_url: AnyUrl = None,
    filepath_delimiter: str = "/",
) -> None:
    emitter = DatahubRestEmitter(
        gms_server=settings.DATAHUB_METADATA_SERVER_URL,
        token=settings.DATAHUB_ACCESS_TOKEN,
    )

    data_platform_urn = builder.make_data_platform_urn(platform=platform)

    data_platform_info = DataPlatformInfo(
        name=platform,
        displayName=display_name,
        logoUrl=logo_url,
        datasetNameDelimiter=filepath_delimiter,
        type=PlatformTypeClass.OTHERS,
    )
    data_platform_info_mcp = MetadataChangeProposalWrapper(
        entityUrn=data_platform_urn,
        aspect=data_platform_info,
    )
    emitter.emit_mcp(data_platform_info_mcp)


if __name__ == "__main__":
    add_platform_metadata(
        platform="deltaLake",
        display_name="Delta Lake",
        logo_url="https://delta.io/static/3bd8fea55ff57287371f4714232cd4ef/ac8f8/delta-lake-logo.webp",
        filepath_delimiter="/",
    )
