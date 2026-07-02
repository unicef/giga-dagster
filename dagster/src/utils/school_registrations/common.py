from typing import Any

import requests
from models.file_upload import FileUpload
from pyspark import sql
from pyspark.sql import functions as f
from sqlalchemy import select

from src.settings import settings
from src.utils.db.primary import get_db_context
from src.utils.nocodb.get_nocodb_data import (
    create_nocodb_table_record,
    get_nocodb_table_id_from_name,
)


def _spark_row_to_dict(row: Any, columns: list[str]) -> dict[str, Any]:
    """Convert a Spark Row into a plain dict using the DataFrame column order."""
    return {column: row[column] for column in columns}


def set_school_registration_verification_status(
    df: sql.DataFrame,
    uploaded_columns: list[str],
    source: str = None,
) -> sql.DataFrame:
    """Set or fill verification_status based on the registration source."""
    default_value = "unverified" if source == "gigameter" else "verified"
    if "verification_status" in uploaded_columns:
        return df.withColumn(
            "verification_status",
            f.coalesce(f.col("verification_status"), f.lit(default_value)),
        )
    return df.withColumn("verification_status", f.lit(default_value))


def process_school_registration_dq_result(
    context: Any,
    upload_id: str,
    country_iso3_code: str,
    row_count: int,
    df_passed: Any,
    dq_results: Any,
) -> None:
    """Notify downstream registration systems after GigaMeter geolocation DQ."""
    with get_db_context() as db:
        file_upload = db.scalar(
            select(FileUpload).where(FileUpload.id == upload_id),
        )

    if not file_upload or file_upload.source != "gigameter":
        return

    if row_count > 0:
        school_row = df_passed.first()
        school_data = _spark_row_to_dict(school_row, df_passed.columns)
        write_to_nocodb(context, country_iso3_code, school_data)
        return

    school_row = dq_results.first()
    if not school_row:
        return

    school_data = _spark_row_to_dict(school_row, dq_results.columns)
    school_id_giga = school_data.get("school_id_giga") or school_data.get(
        "giga_id_school"
    )
    if school_id_giga:
        call_gigameter_soft_delete(
            context,
            school_id_giga,
            school_data.get("failure_reason"),
        )


def write_to_nocodb(
    context: Any,
    country_iso3_code: str,
    school_data: dict[str, Any],
) -> None:
    """Create an unverified school registration record in NoCoDB."""
    if not settings.NOCODB_BASE_URL or not settings.NOCODB_TOKEN:
        context.log.warning(
            "NOCODB_BASE_URL or NOCODB_TOKEN not configured. Skipping NoCoDB write."
        )
        return

    try:
        giga_id_school = school_data.get("school_id_giga") or school_data.get(
            "giga_id_school"
        )
        table_id = get_nocodb_table_id_from_name("SchoolRegistrations")

        record_data = {
            "giga_id_school": giga_id_school,
            "school_id": school_data.get("school_id")
            or school_data.get("school_id_govt"),
            "school_name": school_data.get("school_name", ""),
            "latitude": school_data.get("latitude", ""),
            "longitude": school_data.get("longitude", ""),
            "country_iso3_code": country_iso3_code,
            "education_level": school_data.get("education_level", ""),
            "contact_name": school_data.get("contact_name", ""),
            "contact_email": school_data.get("contact_email", ""),
            "verification_status": "unverified",
        }
        create_nocodb_table_record(table_id, record_data)
        context.log.info(f"NoCoDB write successful for giga_id_school={giga_id_school}")
    except Exception as exc:
        context.log.error(f"NoCoDB write failed: {exc}")


def call_gigameter_soft_delete(
    context: Any, school_id_giga: str, failure_reason: str = None
) -> None:
    """Mark a GigaMeter school registration as rejected after failed DQ."""
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
    payload = {"giga_id_school": school_id_giga, "is_deleted": True}

    try:
        response = requests.put(url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        context.log.error(
            f"GigaMeter soft-delete failed for school_id_giga={school_id_giga}: {exc}"
        )
