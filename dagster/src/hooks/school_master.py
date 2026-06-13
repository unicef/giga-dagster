from models.file_upload import DQStatusEnum, FileUpload
from sqlalchemy import update
from sqlalchemy.orm import Session

from dagster import HookContext, failure_hook, success_hook
from src.utils.adls import ADLSFileClient
from src.utils.db.primary import get_db_context
from src.utils.op_config import FileConfig


@success_hook
def school_dq_checks_location_db_update_hook(context: HookContext):
    """Updates the Ingestion Portal database entry with the ADLS location of the DQ checks summary."""
    if not context.step_key.endswith("_data_quality_results_summary"):
        return

    context.log.info("Running database update hook for DQ checks location...")
    config = FileConfig(**context.op_config)

    values = {
        FileUpload.dq_report_path: str(config.destination_filepath_object),
        FileUpload.dq_status: DQStatusEnum.COMPLETED,
    }

    # Hooks do not receive op outputs, so read back the summary JSON from ADLS
    # to feed the row counts into the portal DB. If this fails, fall back to the
    # original UPDATE without the counts.
    try:
        report = ADLSFileClient().download_json(str(config.destination_filepath_object))
        summary = (report or {}).get("summary", {})
        values.update(
            {
                FileUpload.rows: summary.get("rows"),
                FileUpload.rows_passed: summary.get("rows_passed"),
                FileUpload.rows_failed: summary.get("rows_failed"),
            }
        )
    except Exception as e:
        context.log.error(
            f"Failed to read DQ summary report from ADLS; "
            f"skipping row counts update: {e}"
        )

    with get_db_context() as db:
        db.execute(
            update(FileUpload)
            .where(FileUpload.id == config.filename_components.id)
            .values(values),
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

    db: Session
    with get_db_context() as db:
        with db.begin():
            db.execute(
                update(FileUpload)
                .where(FileUpload.id == config.filename_components.id)
                .values({FileUpload.dq_status: DQStatusEnum.ERROR})
            )
