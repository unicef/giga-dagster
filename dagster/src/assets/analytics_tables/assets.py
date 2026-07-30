from pathlib import Path

import sqlparse
from sqlalchemy import text
from sqlalchemy.orm import Session
from src.settings import settings
from src.utils.db.trino import get_db_context

from dagster import AssetKey, OpExecutionContext, asset

QUERIES_ROOT = settings.BASE_DIR / "src" / "queries" / "analytics_tables"
DELTA_LAKE_CATALOG = "delta_lake"
DELTA_LAKE_SCHEMA = "default"


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


def _table_exists(db: Session, table_name: str) -> bool:
    result = db.execute(
        text(
            f"SELECT 1 FROM {DELTA_LAKE_CATALOG}.information_schema.tables "  # nosec B608
            "WHERE table_schema = :schema AND table_name = :table_name"
        ),
        {"schema": DELTA_LAKE_SCHEMA, "table_name": table_name},
    ).first()
    return result is not None


def _read_statements(path: Path) -> list[str]:
    return _split_statements(path.read_text())


def _execute_statements(
    context: OpExecutionContext, db: Session, statements: list[str]
) -> None:
    for i, statement in enumerate(statements):
        context.log.info(f"executing statement {i + 1}/{len(statements)}")
        db.execute(text(statement))
    db.commit()


def _run_daily(context: OpExecutionContext) -> None:
    name = context.asset_key.path[-1]
    with get_db_context() as db:
        _execute_statements(
            context, db, _read_statements(QUERIES_ROOT / "daily" / f"{name}.sql")
        )


def _run_incremental(context: OpExecutionContext) -> None:
    name = context.asset_key.path[-1]
    with get_db_context() as db:
        exists = _table_exists(db, name)
        subdir = "update" if exists else "create"
        context.log.info(
            f"table {DELTA_LAKE_SCHEMA}.{name} "
            + (
                "exists, running update script"
                if exists
                else "does not exist, running create script"
            )
        )
        _execute_statements(
            context,
            db,
            _read_statements(QUERIES_ROOT / "incremental" / subdir / f"{name}.sql"),
        )


# =============================================================================
# Daily assets
# =============================================================================


@asset(key_prefix=["daily"], group_name="daily", compute_kind="trino")
def country_versions(context: OpExecutionContext) -> None:
    _run_daily(context)


@asset(key_prefix=["daily"], group_name="daily", compute_kind="trino")
def all_gmeter_only_measurements(context: OpExecutionContext) -> None:
    _run_daily(context)


@asset(key_prefix=["daily"], group_name="daily", compute_kind="trino")
def all_mlab_only_measurements(context: OpExecutionContext) -> None:
    _run_daily(context)


@asset(key_prefix=["daily"], group_name="daily", compute_kind="trino")
def all_ping_hourly(context: OpExecutionContext) -> None:
    _run_daily(context)


@asset(key_prefix=["daily"], group_name="daily", compute_kind="trino")
def bra_nicbr_daily_tb(context: OpExecutionContext) -> None:
    _run_daily(context)


@asset(key_prefix=["daily"], group_name="daily", compute_kind="trino")
def bra_nicbr_registered_tb(context: OpExecutionContext) -> None:
    _run_daily(context)


@asset(key_prefix=["daily"], group_name="daily", compute_kind="trino")
def all_gigamaps_realtimeconnectivity(context: OpExecutionContext) -> None:
    _run_daily(context)


@asset(
    key_prefix=["daily"],
    group_name="daily",
    deps=[AssetKey(["daily", "country_versions"])],
    compute_kind="trino",
)
def all_school_master(context: OpExecutionContext) -> None:
    _run_daily(context)


@asset(
    key_prefix=["daily"],
    group_name="daily",
    deps=[AssetKey(["daily", "all_ping_hourly"])],
    compute_kind="trino",
)
def all_ping_daily(context: OpExecutionContext) -> None:
    _run_daily(context)


@asset(
    key_prefix=["daily"],
    group_name="daily",
    deps=[AssetKey(["daily", "bra_nicbr_daily_tb"])],
    compute_kind="trino",
)
def bra_benchmarkstatus_wow(context: OpExecutionContext) -> None:
    _run_daily(context)


@asset(
    key_prefix=["daily"],
    group_name="daily",
    deps=[
        AssetKey(["daily", "all_gmeter_only_measurements"]),
        AssetKey(["daily", "all_mlab_only_measurements"]),
    ],
    compute_kind="trino",
)
def all_gigameter_valid_test_checker(context: OpExecutionContext) -> None:
    _run_daily(context)


@asset(
    key_prefix=["daily"],
    group_name="daily",
    deps=[
        AssetKey(["daily", "all_gigameter_valid_test_checker"]),
        AssetKey(["daily", "all_gmeter_only_measurements"]),
        AssetKey(["daily", "all_mlab_only_measurements"]),
        AssetKey(["daily", "all_school_master"]),
    ],
    compute_kind="trino",
)
def all_gigameter_measurement_data(context: OpExecutionContext) -> None:
    _run_daily(context)


@asset(
    key_prefix=["daily"],
    group_name="daily",
    deps=[
        AssetKey(["daily", "all_gmeter_only_measurements"]),
        AssetKey(["daily", "all_mlab_only_measurements"]),
        AssetKey(["daily", "all_school_master"]),
    ],
    compute_kind="trino",
)
def all_gigameter_measurement_data_tb_physical(context: OpExecutionContext) -> None:
    _run_daily(context)


@asset(
    key_prefix=["daily"],
    group_name="daily",
    deps=[AssetKey(["daily", "all_gigameter_measurement_data"])],
    compute_kind="trino",
)
def all_gigameter_measurement_data_daily(context: OpExecutionContext) -> None:
    _run_daily(context)


@asset(
    key_prefix=["daily"],
    group_name="daily",
    deps=[AssetKey(["daily", "all_gigameter_measurement_data"])],
    compute_kind="trino",
)
def all_gigameter_measurement_data_weekly(context: OpExecutionContext) -> None:
    _run_daily(context)


@asset(
    key_prefix=["daily"],
    group_name="daily",
    deps=[
        AssetKey(["daily", "all_gigameter_measurement_data"]),
        AssetKey(["daily", "all_school_master"]),
    ],
    compute_kind="trino",
)
def all_gigameter_appversion_funnel(context: OpExecutionContext) -> None:
    _run_daily(context)


@asset(
    key_prefix=["daily"],
    group_name="daily",
    deps=[AssetKey(["daily", "all_school_master"])],
    compute_kind="trino",
)
def all_gigameter_funnelsummary_tb_physical(context: OpExecutionContext) -> None:
    _run_daily(context)


@asset(
    key_prefix=["daily"],
    group_name="daily",
    deps=[
        AssetKey(["daily", "all_gigameter_measurement_data"]),
        AssetKey(["daily", "all_gmeter_only_measurements"]),
    ],
    compute_kind="trino",
)
def all_gigameter_school_consistency_history(context: OpExecutionContext) -> None:
    _run_daily(context)


@asset(
    key_prefix=["daily"],
    group_name="daily",
    deps=[
        AssetKey(["daily", "all_gigameter_measurement_data"]),
        AssetKey(["daily", "all_school_master"]),
    ],
    compute_kind="trino",
)
def all_gigameter_registered_schools(context: OpExecutionContext) -> None:
    _run_daily(context)


@asset(
    key_prefix=["daily"],
    group_name="daily",
    deps=[
        AssetKey(["daily", "all_gigameter_appversion_funnel"]),
        AssetKey(["daily", "all_gigameter_measurement_data"]),
        AssetKey(["daily", "all_school_master"]),
    ],
    compute_kind="trino",
)
def all_gigameter_registered_devices(context: OpExecutionContext) -> None:
    _run_daily(context)


@asset(
    key_prefix=["daily"],
    group_name="daily",
    deps=[
        AssetKey(["daily", "all_gigameter_appversion_funnel"]),
        AssetKey(["daily", "all_gigameter_measurement_data_tb_physical"]),
        AssetKey(["daily", "all_school_master"]),
    ],
    compute_kind="trino",
)
def all_gigameter_registered_tb_physical(context: OpExecutionContext) -> None:
    _run_daily(context)


@asset(
    key_prefix=["daily"],
    group_name="daily",
    deps=[
        AssetKey(["daily", "all_gigameter_appversion_funnel"]),
        AssetKey(["daily", "all_gigameter_measurement_data"]),
        AssetKey(["daily", "all_ping_daily"]),
    ],
    compute_kind="trino",
)
def all_gigameter_inc_ping_daily(context: OpExecutionContext) -> None:
    _run_daily(context)


@asset(
    key_prefix=["daily"],
    group_name="daily",
    deps=[AssetKey(["daily", "all_gigameter_measurement_data_tb_physical"])],
    compute_kind="trino",
)
def mng_gigameter_qos_measurements(context: OpExecutionContext) -> None:
    _run_daily(context)


@asset(
    key_prefix=["daily"],
    group_name="daily",
    deps=[AssetKey(["daily", "all_gigameter_registered_tb_physical"])],
    compute_kind="trino",
)
def mng_gigameter_qos_registered(context: OpExecutionContext) -> None:
    _run_daily(context)


# =============================================================================
# Incremental assets
# =============================================================================


@asset(key_prefix=["incremental"], group_name="incremental", compute_kind="trino")
def all_gmeter_only_measurements_incremental(context: OpExecutionContext) -> None:
    _run_incremental(context)


@asset(key_prefix=["incremental"], group_name="incremental", compute_kind="trino")
def all_mlab_only_measurements_incremental(context: OpExecutionContext) -> None:
    _run_incremental(context)


@asset(
    key_prefix=["incremental"],
    group_name="incremental",
    deps=[
        AssetKey(["incremental", "all_gmeter_only_measurements_incremental"]),
        AssetKey(["incremental", "all_mlab_only_measurements_incremental"]),
    ],
    compute_kind="trino",
)
def all_gigameter_valid_test_checker_incremental(context: OpExecutionContext) -> None:
    _run_incremental(context)


@asset(
    key_prefix=["incremental"],
    group_name="incremental",
    deps=[
        AssetKey(["incremental", "all_gigameter_valid_test_checker_incremental"]),
        AssetKey(["incremental", "all_gmeter_only_measurements_incremental"]),
        AssetKey(["incremental", "all_mlab_only_measurements_incremental"]),
        AssetKey(["daily", "all_school_master"]),
    ],
    compute_kind="trino",
)
def all_gigameter_measurement_data_incremental(context: OpExecutionContext) -> None:
    _run_incremental(context)
