"""Aggregate govt→Giga value mapping stats for the DQ report PDF."""

from __future__ import annotations

from pyspark.sql import DataFrame as SqlDataFrame
from pyspark.sql import functions as f

from src.settings import settings
from src.utils.nocodb.get_nocodb_data import get_nocodb_table_rows

# Govt schema columns (values in column_to_schema_mapping) → PDF section keys.
PDF_VALUE_MAP_SECTIONS: dict[str, str] = {
    "education_level_govt": "education",
    "electricity_availability": "electricity",
    "electricity_type_govt": "electricity",
    "connectivity_govt": "connectivity",
    "connectivity_type_govt": "connectivity",
}

# When multiple govt columns map to the same PDF section, prefer this order.
_SECTION_COLUMN_PRIORITY: dict[str, list[str]] = {
    "education": ["education_level_govt"],
    "electricity": ["electricity_availability", "electricity_type_govt"],
    "connectivity": ["connectivity_govt", "connectivity_type_govt"],
}


def _fmt_count(n: int) -> str:
    return f"{n:,}"


def _fmt_pct(count: int, total: int) -> str:
    if total <= 0:
        return "0%"
    return f"{(count / total) * 100:.1f}%"


def _display_val(val) -> str:
    if val is None:
        return "NA"
    text = str(val).strip()
    if not text or text.lower() in ("nan", "none", "null"):
        return "NA"
    return text


def _nocodb_column_pairs(uploaded_columns: set[str]) -> list[tuple[str, str, str]]:
    """Return (govt_col, giga_col, pdf_section) for uploaded mapping columns."""
    try:
        rows = get_nocodb_table_rows(
            settings.NOCODB_NAME_MAPPINGS_TABLE_ID,
            where="(column_name,notblank)",
            fields="column_name,target_column,table_id",
        )
    except Exception:
        return []

    pairs: list[tuple[str, str, str]] = []
    for row in rows:
        govt_col = (row.get("column_name") or "").strip()
        giga_col = (row.get("target_column") or "").strip()
        section = PDF_VALUE_MAP_SECTIONS.get(govt_col)
        if not section or govt_col not in uploaded_columns:
            continue
        if not giga_col:
            continue
        pairs.append((govt_col, giga_col, section))
    return pairs


def aggregate_value_maps_for_pdf(
    df: SqlDataFrame,
    uploaded_columns: list[str],
) -> dict[str, list[dict[str, str]]]:
    """
    Build valueMaps for passed rows: govt source value → Giga mapped value with count and %.

    Counts are over schools that passed critical DQ checks (dq_has_critical_error == 0),
    matching the Mexico report copy.
    """
    if "dq_has_critical_error" not in df.columns:
        return {}

    passed = df.filter(f.col("dq_has_critical_error") == 0)
    total = passed.count()
    if total == 0:
        return {}

    uploaded_set = set(uploaded_columns)
    pairs = _nocodb_column_pairs(uploaded_set)
    if not pairs:
        return {}

    result: dict[str, list[dict[str, str]]] = {}
    filled_sections: set[str] = set()

    for section, preferred_cols in _SECTION_COLUMN_PRIORITY.items():
        section_pairs = [
            (govt, giga)
            for govt, giga, sec in pairs
            if sec == section and govt in df.columns and giga in df.columns
        ]
        if not section_pairs:
            continue

        ordered: list[tuple[str, str]] = []
        for col in preferred_cols:
            for govt, giga in section_pairs:
                if govt == col and (govt, giga) not in ordered:
                    ordered.append((govt, giga))
        for govt, giga in section_pairs:
            if (govt, giga) not in ordered:
                ordered.append((govt, giga))

        for govt_col, giga_col in ordered:
            if section in filled_sections:
                break
            rows = (
                passed.groupBy(govt_col, giga_col)
                .count()
                .orderBy(f.desc("count"))
                .collect()
            )
            if not rows:
                continue

            result[section] = [
                {
                    "src": _display_val(row[govt_col]),
                    "dst": _display_val(row[giga_col]),
                    "count": _fmt_count(int(row["count"])),
                    "pct": _fmt_pct(int(row["count"]), total),
                }
                for row in rows
            ]
            filled_sections.add(section)

    return result
