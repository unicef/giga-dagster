from models.file_upload import FileUpload
from sqlalchemy import update

from dagster import HookContext, success_hook
from src.utils.db import get_db_context
from src.utils.op_config import FileConfig


@success_hook
def geolocation_dq_checks_location_db_update_hook(context: HookContext):
    if context.step_key != "geolocation_data_quality_results_summary":
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
                }
            )
        )
        db.commit()

    context.log.info("Database update hook OK")
