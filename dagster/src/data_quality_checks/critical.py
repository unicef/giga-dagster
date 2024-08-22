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

    human_readeable_mapping = {
        "dq_is_null_mandatory-school_id_govt": "Non-nullable column school_id_govt is null",
        "dq_duplicate-school_id_govt": "Column school_id_govt has a duplicate",
        "dq_duplicate-school_id_giga": "Column school_id_giga has a duplicate",
        "dq_is_invalid_range-latitude": "Column latitude is not between -90 and 90",
        "dq_is_invalid_range-longitude": "Column longitude is not between -180 and 180",
        "dq_is_not_within_country": "Coordinates is not within the country",
        "dq_is_not_create": "Tried creating a new school_id_giga that already exists - must use UPDATE instead",
        "dq_is_not_update": "Tried updating a school_id_giga that does not exist - must use CREATE instead",
    }

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
                        f.lit(human_readeable_mapping.get(c, c[3:])),
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
