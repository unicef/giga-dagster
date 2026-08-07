"""
Utility for generating DQ Kit ZIP bundles using convention-based paths.
"""

import csv
import io
import zipfile

from dagster import OpExecutionContext
from src.constants import constants
from src.utils.adls import ADLSFileClient
from src.utils.filename import deconstruct_school_master_filename_components


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


def _count_csv_data_rows(data: bytes) -> int:
    """Count data rows in a CSV (excluding the header), handling quoted newlines."""
    text = data.decode("utf-8-sig", errors="replace")
    rows = sum(1 for _ in csv.reader(io.StringIO(text)))
    return max(rows - 1, 0)


def _display_stem(country_code: str, upload_id: str, dataset: str, stem: str) -> str:
    """User-facing stem for the approved/rejected CSVs inside the kit:
    <ISO>_<timestamp>_<upload_id>_<dataset>[_<source>].

    Falls back to the original stem if it doesn't follow the upload naming
    convention (blob paths are unaffected either way).
    """
    try:
        components = deconstruct_school_master_filename_components(f"{stem}.csv")
        timestamp = components.timestamp.strftime("%Y%m%d-%H%M%S")
    except Exception:
        return stem

    elements = [country_code, timestamp, upload_id, dataset]
    if components.source:
        elements.append(components.source)
    return "_".join(elements)


def _collect_row_csv_entry(
    adls_client: ADLSFileClient,
    blob_path: str,
    label: str,
    folder: str,
    stem: str,
    description: str,
    entries: list[tuple[str, bytes]],
    sections: list[tuple[str, list[str]]],
    context: OpExecutionContext,
) -> None:
    """Add an approved/rejected rows CSV entry, skipping files with 0 data rows.

    The filename embeds the label and row count so approved/rejected files
    never share an identical name.
    """
    data = _safe_download(adls_client, blob_path, context)
    if data is None:
        return

    count = _count_csv_data_rows(data)
    if count == 0:
        context.log.info(f"Skipped {label} rows: file has 0 data rows")
        return

    entries.append((f"{folder}/{label}_{stem}_{count}_rows.csv", data))
    sections.append((f"{folder}/", [f"*.csv   - {description} ({count:,} rows)"]))
    context.log.info(f"Added {label} rows ({count} rows)")


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

    Includes (each section only when the artifact exists and is non-empty):
    - Raw data
    - DQ summary (JSON)
    - Approved/rejected rows (human-readable CSV, row count in filename)
    - Map HTML
    - README describing the actual ZIP contents
    """
    dataset_prefix = f"school-{dataset}"
    dq_root = f"{constants.dq_results_folder}/{dataset_prefix}"

    # Convention-based paths matching the existing sensor mappings
    paths = {
        "raw_data": f"{constants.UPLOAD_PATH_PREFIX}/{dataset_prefix}/{country_code}/{original_filename}",
        "dq_summary_json": f"{dq_root}/dq-summary/{country_code}/{stem}.json",
        "passed_rows": f"{dq_root}/dq-passed-rows-human-readable/{country_code}/{stem}.csv",
        "failed_rows": f"{dq_root}/dq-failed-rows-human-readable/{country_code}/{stem}.csv",
        "map_html": f"{dq_root}/dq-map/{country_code}/school_map_{country_code}_{stem}.html",
        "master_export": f"{dq_root}/master-export/{country_code}/{stem}.csv",
    }

    context.log.info(f"Generating DQ Kit ZIP for {country_code}/{upload_id}")

    # Collect entries first so the README can describe the actual contents.
    entries: list[tuple[str, bytes]] = []
    sections: list[tuple[str, list[str]]] = []

    # Raw data
    if data := _safe_download(adls_client, paths["raw_data"], context):
        entries.append((f"1_raw_data/{original_filename}", data))
        sections.append(("1_raw_data/", ["Original uploaded file (as submitted)"]))
        context.log.info("Added raw_data")

    # DQ summary (json)
    if data := _safe_download(adls_client, paths["dq_summary_json"], context):
        entries.append((f"2_dq_summary/{stem}.json", data))
        sections.append(
            ("2_dq_summary/", ["*.json  - Data quality summary (machine-readable)"])
        )
        context.log.info("Added dq_summary_json")

    # Approved/rejected rows (skipped entirely when there are zero data rows)
    row_csv_stem = _display_stem(country_code, upload_id, dataset, stem)
    _collect_row_csv_entry(
        adls_client,
        paths["passed_rows"],
        "approved",
        "3_approved_data",
        row_csv_stem,
        "Schools APPROVED by all data quality checks",
        entries,
        sections,
        context,
    )
    _collect_row_csv_entry(
        adls_client,
        paths["failed_rows"],
        "rejected",
        "4_rejected_data",
        row_csv_stem,
        "Schools REJECTED by data quality checks, with reasons",
        entries,
        sections,
        context,
    )

    # Map HTML
    if data := _safe_download(adls_client, paths["map_html"], context):
        entries.append((f"5_map_visualization/school_map_{country_code}.html", data))
        sections.append(
            (
                "5_map_visualization/",
                [
                    "*.html  - Interactive map showing approved (green) and",
                    "          rejected (red) schools. Open in a web browser to view.",
                ],
            )
        )
        context.log.info("Added map_html")

    # School master snapshot (only present after the post-approval run has exported it)
    if data := _safe_download(adls_client, paths["master_export"], context):
        entries.append((f"6_school_master/school_master_{country_code}.csv", data))
        sections.append(
            (
                "6_school_master/",
                [
                    "*.csv   - Snapshot of the school_master table for this country,",
                    "          taken immediately after the approved rows were merged.",
                ],
            )
        )
        context.log.info("Added master_export")

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf.writestr(
            "README.txt",
            generate_readme(
                country_code, dataset, upload_id, original_filename, sections
            ),
        )
        context.log.info("Added README.txt")
        for arcname, data in entries:
            zipf.writestr(arcname, data)

    zip_buffer.seek(0)
    zip_bytes = zip_buffer.getvalue()
    filename = f"DQ_Kit_{country_code}_{dataset}_{upload_id}.zip"

    context.log.info(f"Generated ZIP: {filename} ({len(zip_bytes)} bytes)")

    return zip_bytes, filename


_HOW_TO_USE_STEPS = {
    "2_dq_summary/": "Review the DQ summary (2_dq_summary/*.json) for overall statistics.",
    "3_approved_data/": (
        "Check approved data (3_approved_data/*.csv) for schools ready to ingest."
    ),
    "4_rejected_data/": (
        "Review rejected data (4_rejected_data/*.csv) to understand why rows "
        "were rejected."
    ),
    "5_map_visualization/": (
        "Open the map (5_map_visualization/*.html) for visual analysis."
    ),
}


def generate_readme(
    country_code: str,
    dataset: str,
    upload_id: str,
    original_filename: str,
    sections: list[tuple[str, list[str]]],
) -> str:
    """Generate README content describing the actual DQ Kit contents."""
    contents = "\n\n".join(
        folder + "\n" + "\n".join(f"    {line}" for line in lines)
        for folder, lines in sections
    )

    included_folders = {folder for folder, _ in sections}
    steps = "\n".join(
        f"{i}. {step}"
        for i, step in enumerate(
            (
                step
                for folder, step in _HOW_TO_USE_STEPS.items()
                if folder in included_folders
            ),
            start=1,
        )
    )

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

{contents}

================================================================================
                           HOW TO USE
================================================================================

{steps}

================================================================================
Generated by: Giga Sync Data Ingestion Platform
================================================================================
"""
