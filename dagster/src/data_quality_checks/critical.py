from pyspark import sql
from pyspark.sql import functions as f

from dagster import OpExecutionContext
from src.constants import UploadMode
from src.utils.data_quality_descriptions import (
    rename_dq_has_critical_error_columns_nocodb,
)
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
    critical_column_dq_checks = [
        f"dq_is_null_mandatory-{column}" for column in config_column_list
    ]

    # other critical checks per dataset
    if dataset_type in ["master", "geolocation"]:
        critical_column_dq_checks.extend(
            [
                "dq_duplicate-school_id_govt",
                "dq_duplicate-school_id_giga",
                "dq_is_null_optional-latitude",
                "dq_is_null_optional-longitude",
                "dq_is_invalid_range-latitude",
                "dq_is_invalid_range-longitude",
                "dq_is_not_within_country",
            ]
        )
        if mode == UploadMode.CREATE.value:
            critical_column_dq_checks.append("dq_is_not_create")
        elif mode == UploadMode.UPDATE.value:
            critical_column_dq_checks.append("dq_is_not_update")
    elif dataset_type in ["reference", "coverage", "coverage_fb", "coverage_itu"]:
        critical_column_dq_checks.append("dq_duplicate-school_id_giga")
    elif dataset_type == "qos":
        critical_column_dq_checks.extend(
            [
                "dq_duplicate-school_id_giga",
                # "dq_duplicate-school_id_govt",
            ]
        )

    full_human_readable_mapping = rename_dq_has_critical_error_columns_nocodb()

    df = df.withColumns(
        {
            "dq_has_critical_error": f.greatest(
                *[
                    f.when(f.isnan(f.col(c)) | f.col(c).isNull(), f.lit(0)).otherwise(
                        f.col(c)
                    )
                    for c in critical_column_dq_checks
                ]
            ).cast("int"),
            "failure_reason": f.concat_ws(
                ", ",
                *[
                    f.when(
                        f.col(c) == 1,
                        f.lit(full_human_readable_mapping.get(c, c[3:])),
                    )
                    for c in critical_column_dq_checks
                ],
            ),
        }
    )

    print(
        df.select(
            *["dq_has_critical_error", "latitude", "dq_is_null_optional-latitude"]
        ).toPandas()
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

    print(
        df.select(
            *["dq_has_critical_error", "latitude", "dq_is_null_optional-latitude"]
        ).toPandas()
    )

    return df
