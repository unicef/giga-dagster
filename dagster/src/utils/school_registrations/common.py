from typing import Any

import requests

from src.settings import settings
from src.utils.nocodb.get_nocodb_data import (
    create_nocodb_table_record,
    get_nocodb_table_id_from_name,
)


def write_to_nocodb(
    context: Any,
    country_iso3_code: str,
    school_data: dict[str, Any],
) -> None:
    """Write school registration to NoCoDB."""
    if not settings.NOCODB_BASE_URL or not settings.NOCODB_TOKEN:
        context.log.warning(
            "NOCODB_BASE_URL or NOCODB_TOKEN not configured. Skipping NoCoDB write."
        )
        return

    try:
        giga_id_school = school_data.get("giga_id_school")
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
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        context.log.error(
            f"GigaMeter soft-delete failed for school_id_giga={school_id_giga}: {exc}"
        )
