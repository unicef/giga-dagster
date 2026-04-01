from dagster_pyspark import PySparkResource
from delta import DeltaTable
from src.resources import ResourceKey
from src.utils.adls import ADLSFileClient
from src.utils.schema import construct_full_table_name

from dagster import Config, OpExecutionContext, asset


class FuzzyCorrectionsConfig(Config):
    upload_id: str
    corrections_json_path: str
    country_code: str


def _load_corrections(
    context: OpExecutionContext,
    adls_client: ADLSFileClient,
    config: FuzzyCorrectionsConfig,
) -> list[dict]:
    context.log.info(f"Reading corrections from {config.corrections_json_path}")

    data = adls_client.download_json(config.corrections_json_path)
    if not data:
        context.log.info("No corrections found.")
        return []

    corrections = data.get("corrections", [])
    if not corrections:
        context.log.info("Empty corrections list.")

    return corrections


def _build_updates_map(corrections_list: list[dict]) -> dict[str, dict[str, str]]:
    updates_map: dict[str, dict[str, str]] = {}

    for item in corrections_list:
        sid = item.get("school_id_govt")
        col = item.get("column_name")
        val = item.get("final_value")

        if sid and col:
            updates_map.setdefault(sid, {})[col] = val

    return updates_map


def _resolve_dq_table(
    context: OpExecutionContext,
    spark,
    config: FuzzyCorrectionsConfig,
) -> str:
    schema_name = "school_geolocation_dq_results"

    prefix = f"{config.upload_id}_{config.country_code}"
    tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {schema_name}").collect()]

    candidates = [t for t in tables if t.startswith(prefix)]

    if not candidates:
        raise ValueError(f"No DQ results table found for upload {config.upload_id}")

    latest_table_name = sorted(candidates)[-1]
    full_table_name = f"{schema_name}.{latest_table_name}"

    context.log.info(f"Updating table: {full_table_name}")
    return full_table_name


def _build_update_df(spark, updates_map: dict[str, dict[str, str]]):
    rows = []
    updated_cols: set[str] = set()

    for sid, changes in updates_map.items():
        row = {"school_id_govt_key": sid}
        for col, val in changes.items():
            row[col] = val
            updated_cols.add(col)
        rows.append(row)

    return spark.createDataFrame(rows), updated_cols


def _merge_dq_updates(
    context, spark, full_table_name: str, update_df, updated_cols: set[str]
) -> None:
    dt = DeltaTable.forName(spark, full_table_name)

    update_set_expr = {
        col: f"coalesce(source.{col}, target.{col})" for col in updated_cols
    }

    context.log.info(f"Merging updates into {full_table_name}...")

    (
        dt.alias("target")
        .merge(
            update_df.alias("source"),
            "target.school_id_govt = source.school_id_govt_key",
        )
        .whenMatchedUpdate(set=update_set_expr)
        .execute()
    )


def _merge_mismatch_updates(
    context: OpExecutionContext,
    spark,
    updates_map: dict[str, dict[str, str]],
    config: FuzzyCorrectionsConfig,
) -> None:
    rows = [
        {
            "file_upload_id_key": config.upload_id,
            "school_id_govt_key": sid,
            "column_name_key": col,
            "final_value": val,
            "is_accepted": True,
        }
        for sid, changes in updates_map.items()
        for col, val in changes.items()
    ]

    if not rows:
        return

    mismatch_df = spark.createDataFrame(rows)
    table_name = "school_master.fuzzy_mismatches"

    if not DeltaTable.isDeltaTable(
        spark, construct_full_table_name("school_master", "fuzzy_mismatches")
    ):
        context.log.warning(
            f"Table {table_name} not found. Skipping mismatch status update."
        )
        return

    mt = DeltaTable.forName(spark, table_name)

    (
        mt.alias("target")
        .merge(
            mismatch_df.alias("source"),
            "target.file_upload_id = source.file_upload_id_key AND "
            "target.school_id_govt = source.school_id_govt_key AND "
            "target.column_name = source.column_name_key",
        )
        .whenMatchedUpdate(
            set={
                "final_value": "source.final_value",
                "is_accepted": "source.is_accepted",
            }
        )
        .execute()
    )


@asset(required_resource_keys={ResourceKey.ADLS_FILE_CLIENT.value})
def apply_fuzzy_corrections(
    context: OpExecutionContext,
    config: FuzzyCorrectionsConfig,
    spark: PySparkResource,
):
    s = spark.spark_session
    adls_client: ADLSFileClient = context.resources.adls_file_client

    corrections_list = _load_corrections(context, adls_client, config)
    if not corrections_list:
        return

    updates_map = _build_updates_map(corrections_list)
    if not updates_map:
        return

    full_table_name = _resolve_dq_table(context, s, config)

    update_df, updated_cols = _build_update_df(s, updates_map)

    _merge_dq_updates(context, s, full_table_name, update_df, updated_cols)

    _merge_mismatch_updates(context, s, updates_map, config)

    context.log.info("Corrections applied successfully.")
