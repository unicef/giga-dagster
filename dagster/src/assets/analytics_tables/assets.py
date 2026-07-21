import re
from pathlib import Path

import sqlparse
from sqlalchemy import text
from src.settings import settings
from src.utils.db.trino import get_db_context

from dagster import AssetKey, AssetsDefinition, OpExecutionContext, asset

QUERIES_ROOT = settings.BASE_DIR / "analytics_tables_repo" / "analytics-tables"
GROUPS = ("daily", "incremental")

# Matches `default.<table>` and `delta_lake.default.<table>` table references, the
# only two qualified forms used across the source scripts' CREATE TABLE/FROM/JOIN clauses.
_TABLE_REF_PATTERN = re.compile(r"(?:delta_lake\.)?default\.([a-zA-Z0-9_]+)")


def _discover_query_files() -> list[tuple[str, Path]]:
    return [
        (group, path)
        for group in GROUPS
        for path in sorted((QUERIES_ROOT / group).glob("*.sql"))
    ]


def _split_statements(sql_text: str) -> list[str]:
    statements = []
    for raw in sqlparse.split(sql_text):
        stripped = raw.strip()
        if not stripped:
            continue
        parsed = sqlparse.parse(stripped)
        if not parsed or parsed[0].token_first(skip_cm=True) is None:
            continue
        statements.append(stripped.rstrip(";").strip())
    return statements


def _build_analytics_table_asset(
    group: str, path: Path, deps: list[AssetKey]
) -> AssetsDefinition:
    name = path.stem
    statements = _split_statements(path.read_text())

    @asset(
        name=name,
        key_prefix=[group],
        group_name=group,
        deps=deps,
        compute_kind="trino",
    )
    def _analytics_table_asset(context: OpExecutionContext) -> None:
        with get_db_context() as db:
            for i, statement in enumerate(statements):
                context.log.info(f"executing statement {i + 1}/{len(statements)}")
                db.execute(text(statement))
            db.commit()

    return _analytics_table_asset


def _build_analytics_table_assets() -> list[AssetsDefinition]:
    query_files = _discover_query_files()
    table_to_key = {
        path.stem: AssetKey([group, path.stem]) for group, path in query_files
    }

    assets = []
    for group, path in query_files:
        referenced_tables = set(_TABLE_REF_PATTERN.findall(path.read_text()))
        deps = [
            table_to_key[table]
            for table in referenced_tables
            if table in table_to_key and table != path.stem
        ]
        assets.append(_build_analytics_table_asset(group, path, deps))
    return assets


analytics_table_assets: list[AssetsDefinition] = _build_analytics_table_assets()
