"""
Utility for generating DQ Kit ZIP bundles using convention-based paths.
"""

import io
import zipfile

from dagster import OpExecutionContext
from src.constants import constants
from src.utils.adls import ADLSFileClient


def _safe_download(
    adls_client: ADLSFileClient,
    file_path: str,
    context: OpExecutionContext,
) -> bytes | None:
    """Download a file from ADLS, returning None if it doesn't exist."""
    try:
        return adls_client.download_raw(file_path)
    except Exception as e:
        context.log.warning(f"Could not download {file_path}: {e}")
        return None


def generate_dq_kit_zip_bytes(
    country_code: str,
    upload_id: str,
    dataset: str,
    original_filename: str,
    stem: str,
    adls_client: ADLSFileClient,
    context: OpExecutionContext,
) -> tuple[bytes, str]:
    """
    Generate ZIP bundle containing all DQ artifacts using convention-based paths.

    Includes:
    - Raw data
    - DQ reports (TXT, JSON)
    - Passed/failed rows (human-readable CSV)
    - Map HTML
    - README
    """
    zip_buffer = io.BytesIO()
    dataset_prefix = f"school-{dataset}"
    dq_root = f"{constants.dq_results_folder}/{dataset_prefix}"

    # Convention-based paths matching the existing sensor mappings
    paths = {
        "raw_data": f"{constants.UPLOAD_PATH_PREFIX}/{dataset_prefix}/{country_code}/{original_filename}",
        "dq_summary_json": f"{dq_root}/dq-summary/{country_code}/{stem}.json",
        "dq_report_txt": f"{dq_root}/dq-report/{country_code}/{stem}.txt",
        "passed_rows": f"{dq_root}/dq-passed-rows-human-readable/{country_code}/{stem}.csv",
        "failed_rows": f"{dq_root}/dq-failed-rows-human-readable/{country_code}/{stem}.csv",
        "map_html": f"{dq_root}/dq-map/{country_code}/school_map_{country_code}_{stem}.html",
    }

    context.log.info(f"Generating DQ Kit ZIP for {country_code}/{upload_id}")

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
        # README
        zipf.writestr(
            "README.txt",
            generate_readme(country_code, dataset, upload_id, original_filename),
        )
        context.log.info("Added README.txt")

        # Raw data
        if data := _safe_download(adls_client, paths["raw_data"], context):
            zipf.writestr(f"1_raw_data/{original_filename}", data)
            context.log.info("Added raw_data")

        # DQ summary (json + txt)
        if data := _safe_download(adls_client, paths["dq_summary_json"], context):
            zipf.writestr(f"2_dq_summary/{stem}.json", data)
            context.log.info("Added dq_summary_json")

        if data := _safe_download(adls_client, paths["dq_report_txt"], context):
            zipf.writestr(f"2_dq_summary/{stem}.txt", data)
            context.log.info("Added dq_report_txt")

        # Passed rows
        if data := _safe_download(adls_client, paths["passed_rows"], context):
            zipf.writestr(f"3_accepted_data/{stem}.csv", data)
            context.log.info("Added passed_rows")

        # Failed rows
        if data := _safe_download(adls_client, paths["failed_rows"], context):
            zipf.writestr(f"4_rejected_data/{stem}.csv", data)
            context.log.info("Added failed_rows")

        # Map HTML
        if data := _safe_download(adls_client, paths["map_html"], context):
            zipf.writestr(f"5_map_visualization/school_map_{country_code}.html", data)
            context.log.info("Added map_html")

    zip_buffer.seek(0)
    zip_bytes = zip_buffer.getvalue()
    filename = f"DQ_Kit_{country_code}_{dataset}_{upload_id}.zip"

    context.log.info(f"Generated ZIP: {filename} ({len(zip_bytes)} bytes)")

    return zip_bytes, filename


def generate_readme(
    country_code: str, dataset: str, upload_id: str, original_filename: str
) -> str:
    """Generate README content for the DQ Kit."""
    return f"""\
================================================================================
                          DATA QUALITY KIT
================================================================================

Upload ID:     {upload_id}
Country:       {country_code}
Dataset:       {dataset}
Original File: {original_filename}

================================================================================
                              CONTENTS
================================================================================

1_raw_data/
    Original uploaded file (as submitted)

2_dq_summary/
    *.json  - Data quality summary (machine-readable)
    *.txt   - Data quality report (human-readable)

3_accepted_data/
    *.csv   - Schools that PASSED all data quality checks

4_rejected_data/
    *.csv   - Schools that FAILED data quality checks (with reasons)

5_map_visualization/
    *.html  - Interactive map showing passed (green) and failed (red) schools.
              Open in a web browser to view.

================================================================================
                           HOW TO USE
================================================================================

1. Review the DQ report (2_dq_summary/*.txt) for overall statistics.
2. Check accepted data (3_accepted_data/*.csv) for schools ready to ingest.
3. Review rejected data (4_rejected_data/*.csv) to understand failures.
4. Open the map (5_map_visualization/*.html) for visual analysis.

================================================================================
Generated by: Giga Sync Data Ingestion Platform
================================================================================
"""
