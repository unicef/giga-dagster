from models.file_upload import DQStatusEnum, FileUpload
from sqlalchemy import update

from dagster import HookContext, failure_hook, success_hook
from src.utils.db.primary import get_db_context
from src.utils.op_config import FileConfig


@success_hook
def school_dq_checks_location_db_update_hook(context: HookContext):
    """Updates the Ingestion Portal database entry with the ADLS location of the DQ checks summary."""
    if not context.step_key.endswith("_data_quality_results_summary"):
        return

    context.log.info("Running database update hook for DQ checks location...")
    config = FileConfig(**context.op_config)

    with get_db_context() as db:
        db.execute(
            update(FileUpload)
            .where(FileUpload.id == config.filename_components.id)
            .values(
                {
                    FileUpload.dq_report_path: str(config.destination_filepath_object),
                    FileUpload.dq_status: DQStatusEnum.COMPLETED,
                },
            ),
        )
        db.commit()

    context.log.info("Database update hook OK")


@success_hook
def school_dq_overall_location_db_update_hook(context: HookContext):
    """Updates the Ingestion Portal database entry with the ADLS location of the full DQ checks results."""
    if not context.step_key.endswith("_data_quality_results"):
        return

    context.log.info("Running database update hook for full DQ results location...")
    config = FileConfig(**context.op_config)

    with get_db_context() as db:
        db.execute(
            update(FileUpload)
            .where(FileUpload.id == config.filename_components.id)
            .values(
                {
                    FileUpload.dq_full_path: str(config.destination_filepath_object),
                },
            ),
        )
        db.commit()

    context.log.info("Database update hook OK")


@failure_hook
def school_ingest_error_db_update_hook(context: HookContext):
    """Updates the Ingestion Portal database entry with a failed status."""
    if context.step_key.endswith("_staging"):
        return

    context.log.info("Running database update hook for failed DQ results status...")
    config = FileConfig(**context.op_config)

    with get_db_context() as db:
        db.execute(
            update(FileUpload)
            .where(FileUpload.id == config.filename_components.id)
            .values({FileUpload.dq_status: DQStatusEnum.ERROR})
        )
