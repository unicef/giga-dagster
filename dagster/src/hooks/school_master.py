import requests
from models.file_upload import DQStatusEnum, FileUpload
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from dagster import HookContext, failure_hook, success_hook
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.db.primary import get_db_context
from src.utils.nocodb.get_nocodb_data import (
    create_nocodb_table_record,
    get_nocodb_table_id_from_name,
)
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

        # Write to NoCoDB for GigaMeter registrations
        file_upload = db.scalar(
            select(FileUpload).where(FileUpload.id == config.filename_components.id)
        )

        if file_upload and file_upload.source == "gigameter":
            school_id_giga = file_upload.column_to_schema_mapping.get("school_id_giga")
            if school_id_giga:
                context.log.info(
                    f"DQ checks passed for GigaMeter registration, writing to NoCoDB: {school_id_giga}"
                )
                write_to_nocodb(context, file_upload)

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

        # Call GigaMeter soft-delete for failed registrations
        file_upload = db.scalar(
            select(FileUpload).where(FileUpload.id == config.filename_components.id)
        )

        if file_upload and file_upload.source == "gigameter":
            school_id_giga = file_upload.column_to_schema_mapping.get("school_id_giga")
            if school_id_giga:
                # Get failure reason from context
                failure_reason = f"DQ check failed at step: {context.step_key}"
                if hasattr(context, "op_exception") and context.op_exception:
                    failure_reason = (
                        f"{failure_reason} - Error: {str(context.op_exception)}"
                    )

                context.log.info(
                    f"Calling GigaMeter soft-delete for school_id_giga={school_id_giga}"
                )
                call_gigameter_soft_delete(context, school_id_giga, failure_reason)


def write_to_nocodb(context: HookContext, file_upload: FileUpload) -> None:
    """Write school registration to NoCoDB."""
    if not settings.NOCODB_BASE_URL or not settings.NOCODB_TOKEN:
        context.log.warning(
            "NOCODB_BASE_URL or NOCODB_TOKEN not configured. Skipping NoCoDB write."
        )
        return

    try:
        if not file_upload.metadata_json_path:
            raise ValueError(
                f"No metadata_json_path found for FileUpload {file_upload.id}"
            )

        adls_client = ADLSFileClient()
        metadata_payload = adls_client.download_json(file_upload.metadata_json_path)

        giga_id_school = metadata_payload.get("giga_id_school")
        # Get the NoCoDB table ID for school registrations
        table_id = get_nocodb_table_id_from_name("SchoolRegistrations")

        record_data = {
            "giga_id_school": giga_id_school,
            "school_id": metadata_payload.get("school_id"),
            "school_name": metadata_payload.get("school_name", ""),
            "latitude": metadata_payload.get("latitude", ""),
            "longitude": metadata_payload.get("longitude", ""),
            "country_iso3_code": file_upload.country,
            "education_level": metadata_payload.get("education_level", ""),
            "contact_name": metadata_payload.get("contact_name", ""),
            "contact_email": metadata_payload.get("contact_email", ""),
            "status": "unverified",
        }

        create_nocodb_table_record(table_id, record_data)
        context.log.info(f"NoCoDB write successful for giga_id_school={giga_id_school}")
    except Exception as exc:
        context.log.error(f"NoCoDB write failed: {exc}")


def call_gigameter_soft_delete(
    context: HookContext, school_id_giga: str, failure_reason: str = None
) -> None:
    """Call GigaMeter soft-delete API for a failed school registration."""
    if not settings.GIGAMETER_API_BASE_URL or not settings.GIGAMETER_API_TOKEN:
        context.log.warning(
            "GIGAMETER_API_BASE_URL or GIGAMETER_API_TOKEN not configured. "
            f"Skipping soft-delete for school_id_giga={school_id_giga}"
        )
        return

    url = f"{settings.GIGAMETER_API_BASE_URL}/api/v1/school-registrations/rejection"
    headers = {
        "Authorization": f"Bearer {settings.GIGAMETER_API_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "giga_id_school": school_id_giga,
        "is_deleted": True,
        "rejection_reason": failure_reason or "DQ validation failed",
    }

    try:
        response = requests.put(url, headers=headers, json=payload, timeout=10)
        return response.json()
    except requests.RequestException as exc:
        context.log.error(
            f"GigaMeter soft-delete failed for school_id_giga={school_id_giga}: {exc}"
        )
