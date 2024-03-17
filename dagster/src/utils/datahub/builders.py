import os.path

from datahub.emitter import mce_builder as builder
from src.settings import settings


def build_dataset_urn(filepath: str, platform: str = "adlsGen2"):
    stem, _ = os.path.splitext(filepath)
    platform = builder.make_data_platform_urn(platform)
    return builder.make_dataset_urn(
        platform=platform,
        name=stem.replace("/", "."),
        env=settings.ADLS_ENVIRONMENT,
    )
