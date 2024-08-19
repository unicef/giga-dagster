from functools import reduce
from operator import add

from pyspark import sql
from pyspark.sql import functions as f

from dagster import OpExecutionContext
from src.constants import UploadMode
from src.utils.logger import get_context_with_fallback_logger


def critical_error_checks(
    df: sql.DataFrame,
    dataset_type: str,
    config_column_list: list[str],
    mode=None,
    context: OpExecutionContext = None,
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running critical error checks...")

    # all mandatory columns included in critical error checks
    critial_column_dq_checks = [
        f"dq_is_null_mandatory-{column}" for column in config_column_list
    ]

    # other critical checks per dataset
    if dataset_type in ["master", "geolocation"]:
        critial_column_dq_checks.extend(
            [
                "dq_duplicate-school_id_govt",
                "dq_duplicate-school_id_giga",
                "dq_is_invalid_range-latitude",
                "dq_is_invalid_range-longitude",
                "dq_is_not_within_country",
            ]
        )
        if mode == UploadMode.CREATE.value:
            critial_column_dq_checks.append("dq_is_not_create")
        elif mode == UploadMode.UPDATE.value:
            critial_column_dq_checks.append("dq_is_not_update")
    elif dataset_type in ["reference", "coverage", "coverage_fb", "coverage_itu"]:
        critial_column_dq_checks.append("dq_duplicate-school_id_giga")
    elif dataset_type == "qos":
        critial_column_dq_checks.extend(
            [
                "dq_duplicate-school_id_giga",
                # "dq_duplicate-school_id_govt",
            ]
        )

    df = df.withColumns(
        {
            "dq_has_critical_error": f.when(
                reduce(add, [f.col(c) for c in critial_column_dq_checks]) > 0,
                1,
            ).otherwise(0),
            "failure_reason": f.concat_ws(
                ", ",
                *[
                    f.when(
                        f.col(c) == 1,
                        f.lit(c[3:]),  # remove `dq_` prefix
                    )
                    for c in critial_column_dq_checks
                ],
            ),
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

    ordered_columns.extend(
        [
            "dq_has_critical_error",
            "failure_reason",
            *rest_columns,
        ]
    )
    df = df.select(*ordered_columns).orderBy(
        f.col("dq_has_critical_error").desc(),
        *order_by_columns,
    )

    return df
