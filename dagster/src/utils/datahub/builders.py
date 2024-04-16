from pathlib import Path

from datahub.emitter import mce_builder as builder

from src.settings import settings


def build_dataset_urn(filepath: str, platform: str = "adlsGen2") -> str:
    path = Path(filepath)
    stem = path.parent / path.stem
    platform = builder.make_data_platform_urn(platform)
    return builder.make_dataset_urn(
        platform=platform,
        name=str(stem),
        env=settings.ADLS_ENVIRONMENT,
    )
