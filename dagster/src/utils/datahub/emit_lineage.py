import datahub.emitter.mce_builder as builder
from datahub.emitter.rest_emitter import DatahubRestEmitter

from dagster import OpExecutionContext
from src.settings import settings
from src.utils.datahub.builders import build_dataset_urn
from src.utils.logger import get_context_with_fallback_logger
from src.utils.op_config import FileConfig


def emit_lineage_base(
    upstream_datasets: list[str], dataset_urn: str, context: OpExecutionContext = None
) -> None:
    logger = get_context_with_fallback_logger(context)

    datahub_emitter = DatahubRestEmitter(
        gms_server=settings.DATAHUB_METADATA_SERVER_URL,
        token=settings.DATAHUB_ACCESS_TOKEN,
    )
    upstream_urn_list = []
    for dataset in upstream_datasets:
        if dataset.startswith("urn"):
            upstream_urn = dataset
        else:
            upstream_urn = build_dataset_urn(filepath=dataset)
        upstream_urn_list.append(upstream_urn)

    logger.info(f"upstream_urn_list: {upstream_urn_list}")
    logger.info(f"dataset_urn: {dataset_urn}")

    lineage_mce = builder.make_lineage_mce(
        upstream_urn_list,  # Upstream URNs
        dataset_urn,  # Downstream URN
    )
    logger.info(f"lineage_mce: {lineage_mce}")
    logger.info("EMITTING LINEAGE...")

    datahub_emitter.emit_mce(lineage_mce)

    logger.info("SUCCESS. LINEAGE EMITTED TO DATAHUB.")


def emit_lineage(context: OpExecutionContext) -> None:
    step = context.asset_key.to_user_string()
    context.log.info(f"step: {step}")

    if "raw" not in step:
        config = FileConfig(**context.get_step_execution_context().op_config)

        upstream_dataset_urn = config.datahub_source_dataset_urn
        context.log.info(f"upstream_dataset_urn: {upstream_dataset_urn}")

        dataset_urn = config.datahub_destination_dataset_urn
        context.log.info(f"dataset_urn: {dataset_urn}")

        emit_lineage_base(
            upstream_datasets=[upstream_dataset_urn],
            dataset_urn=dataset_urn,
            context=context,
        )
    else:
        context.log.info("NO LINEAGE SINCE RAW STEP HAS NO UPSTREAM DATASETS.")


if __name__ == "__main__":
    import datetime

    staging_filepath = "staging/school-coverage/AIA/cys5pwept28vrjsgu2bct7lg_AIA_coverage_fb_20240411-074343"
    dataset_urn = build_dataset_urn(filepath=staging_filepath, platform="deltaLake")
    print(dataset_urn)

    files_for_review = [
        {
            "name": "bronze/school-coverage/AIA/eeb8id1vqhwjhq91poln37l4_AIA_coverage_itu_20240411-070143.csv",
            "owner": "$superuser",
            "group": "$superuser",
            "permissions": "rw-r-----",
            "last_modified": datetime.datetime(2024, 4, 12, 7, 6, 57),
            "is_directory": False,
            "etag": "0x8DC5ABF2497E6A3",
            "content_length": 2141,
            "creation_time": datetime.datetime(
                2024, 4, 12, 7, 6, 57, 445673, tzinfo=datetime.UTC
            ),
            "expiry_time": None,
            "encryption_scope": None,
            "encryption_context": None,
        },
        {
            "name": "bronze/school-coverage/AIA/cys5pwept28vrjsgu2bct7lg_AIA_coverage_fb_20240411-074343.csv",
            "owner": "$superuser",
            "group": "$superuser",
            "permissions": "rw-r-----",
            "last_modified": datetime.datetime(2024, 4, 16, 9, 18, 46),
            "is_directory": False,
            "etag": "0x8DC5DF638026ED5",
            "content_length": 1072,
            "creation_time": datetime.datetime(
                2024, 4, 16, 9, 18, 45, 723851, tzinfo=datetime.UTC
            ),
            "expiry_time": None,
            "encryption_scope": None,
            "encryption_context": None,
        },
    ]
    upstream_filepaths = [file_info.get("name") for file_info in files_for_review]
    print(upstream_filepaths)

    emit_lineage_base(
        upstream_datasets=upstream_filepaths,
        dataset_urn=dataset_urn,
    )
