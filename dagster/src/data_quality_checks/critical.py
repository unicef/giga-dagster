from pyspark import sql
from pyspark.sql import functions as f

from dagster import OpExecutionContext
from src.utils.data_quality_descriptions import (
    handle_rename_dq_has_critical_error_column,
)
from src.utils.logger import get_context_with_fallback_logger
from src.utils.nocodb.get_nocodb_data import (
    get_nocodb_table_as_pandas_dataframe,
    get_nocodb_table_id_from_name,
)


def _build_geolocation_critical_exprs(
    df: sql.DataFrame,
    full_human_readable_mapping: dict,
) -> tuple[list, list]:
    """Build critical error expressions for geolocation/master datasets from NocoDB config."""
    noco_table_id = get_nocodb_table_id_from_name(
        table_name="SchoolGeolocationMasterDQChecks"
    )
    noco_table = get_nocodb_table_as_pandas_dataframe(table_id=noco_table_id)

    critical_rows = noco_table[
        noco_table["Critical For"].isin(["always", "create_only"])
    ]
    always_checks = critical_rows[critical_rows["Critical For"] == "always"][
        "DQ Table Column Name"
    ].tolist()
    create_only_checks = critical_rows[critical_rows["Critical For"] == "create_only"][
        "DQ Table Column Name"
    ].tolist()

    critical_exprs = []
    failure_exprs = []

    for c in always_checks:
        if c not in df.columns:
            continue
        critical_exprs.append(
            f.when(f.isnan(f.col(c)) | f.col(c).isNull(), f.lit(0)).otherwise(f.col(c))
        )
        failure_exprs.append(
            f.when(f.col(c) == 1, f.lit(full_human_readable_mapping.get(c, c[3:])))
        )

    for c in create_only_checks:
        if c not in df.columns:
            continue
        critical_exprs.append(
            f.when(f.col("is_new_school") & (f.col(c) == 1), f.lit(1)).otherwise(
                f.lit(0)
            )
        )
        failure_exprs.append(
            f.when(
                f.col("is_new_school") & (f.col(c) == 1),
                f.lit(full_human_readable_mapping.get(c, c[3:])),
            )
        )

    return critical_exprs, failure_exprs


def _build_master_location_exprs(
    df: sql.DataFrame,
    full_human_readable_mapping: dict,
    already_covered: list[str],
) -> tuple[list, list]:
    """Add mandatory lat/lng checks for master datasets if not already in NocoDB config."""
    critical_exprs = []
    failure_exprs = []
    for c in ["dq_is_null_mandatory-latitude", "dq_is_null_mandatory-longitude"]:
        if c not in df.columns or c in already_covered:
            continue
        critical_exprs.append(
            f.when(f.isnan(f.col(c)) | f.col(c).isNull(), f.lit(0)).otherwise(f.col(c))
        )
        failure_exprs.append(
            f.when(f.col(c) == 1, f.lit(full_human_readable_mapping.get(c, c[3:])))
        )
    return critical_exprs, failure_exprs


def _build_other_dataset_exprs(
    df: sql.DataFrame,
    dataset_type: str,
    config_column_list: list[str],
    full_human_readable_mapping: dict,
) -> tuple[list, list]:
    """Build critical error expressions for non-geolocation datasets."""
    critical_column_dq_checks = [
        f"dq_is_null_mandatory-{column}" for column in config_column_list
    ]

    if dataset_type in ["reference", "coverage", "coverage_fb", "coverage_itu"]:
        critical_column_dq_checks.append("dq_duplicate-school_id_giga")
    elif dataset_type == "qos":
        critical_column_dq_checks.extend(["dq_duplicate-school_id_giga"])

    critical_exprs = [
        f.when(f.isnan(f.col(c)) | f.col(c).isNull(), f.lit(0)).otherwise(f.col(c))
        for c in critical_column_dq_checks
        if c in df.columns
    ]
    failure_exprs = [
        f.when(f.col(c) == 1, f.lit(full_human_readable_mapping.get(c, c[3:])))
        for c in critical_column_dq_checks
        if c in df.columns
    ]
    return critical_exprs, failure_exprs


def critical_error_checks(
    df: sql.DataFrame,
    dataset_type: str,
    config_column_list: list[str],
    context: OpExecutionContext = None,
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running critical error checks...")

    full_human_readable_mapping = handle_rename_dq_has_critical_error_column(
        config_column_list
    )

    if dataset_type in ["master", "geolocation"]:
        critical_exprs, failure_exprs = _build_geolocation_critical_exprs(
            df, full_human_readable_mapping
        )
        if dataset_type == "master":
            noco_table_id = get_nocodb_table_id_from_name(
            table_name="SchoolGeolocationMasterDQChecks"
            )
            noco_table = get_nocodb_table_as_pandas_dataframe(table_id=noco_table_id)
            critical_rows = noco_table[
                noco_table["Critical For"].isin(["always", "create_only"])
            ]
            covered = critical_rows["DQ Table Column Name"].tolist()
            loc_exprs, loc_fail = _build_master_location_exprs(
                df, full_human_readable_mapping, covered
            )
            critical_exprs.extend(loc_exprs)
            failure_exprs.extend(loc_fail)
    else:
        critical_exprs, failure_exprs = _build_other_dataset_exprs(
            df, dataset_type, config_column_list, full_human_readable_mapping
        )

    df = df.withColumns(
        {
            "dq_has_critical_error": f.greatest(*critical_exprs).cast("int"),
            "failure_reason": f.concat_ws(", ", *failure_exprs),
        }
    )

    rest_columns = df.columns
    dataset_has_school_id_giga = "school_id_giga" in rest_columns
    dataset_has_school_id_govt = "school_id_govt" in rest_columns

    for col in [
        "dq_has_critical_error",
        "failure_reason",
        "school_id_giga",
        "school_id_govt",
    ]:
        if col in rest_columns:
            idx = rest_columns.index(col)
            rest_columns.pop(idx)

    ordered_columns = []
    order_by_columns = []
    if dataset_has_school_id_govt:
        ordered_columns.append("school_id_govt")
        order_by_columns.append("school_id_govt")
    if dataset_has_school_id_giga:
        ordered_columns.append("school_id_giga")
        order_by_columns.append("school_id_giga")

    ordered_columns.extend(["dq_has_critical_error", "failure_reason", *rest_columns])
    df = df.select(*ordered_columns).orderBy(
        f.col("dq_has_critical_error").desc(),
        *order_by_columns,
    )

    return df
