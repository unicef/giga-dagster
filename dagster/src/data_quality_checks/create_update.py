from pyspark import sql
from pyspark.sql import functions as f

from dagster import OpExecutionContext
from src.utils.logger import get_context_with_fallback_logger


def add_is_new_entity(
    bronze: sql.DataFrame,
    silver: sql.DataFrame,
    govt_id_column: str,
    output_column: str,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    """Mark rows whose government identifier is absent from the current dataset."""
    logger = get_context_with_fallback_logger(context)
    logger.info(f"Deriving {output_column} from current dataset join...")

    existing_id = f"_existing_{govt_id_column}"
    silver_ids = silver.select(f.col(govt_id_column).alias(existing_id)).dropDuplicates(
        [existing_id]
    )
    joined = bronze.alias("bronze").join(
        silver_ids,
        f.col(f"bronze.{govt_id_column}") == f.col(existing_id),
        how="left",
    )
    return joined.withColumn(output_column, f.col(existing_id).isNull()).select(
        "bronze.*", output_column
    )


def add_is_new_school(
    bronze: sql.DataFrame,
    silver: sql.DataFrame,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    """Backward-compatible school wrapper around ``add_is_new_entity``.

    Kept on ``school_id_govt`` for the legacy school geolocation path until that
    asset pack is removed. Facility bronze uses ``add_is_new_entity`` directly.
    """
    return add_is_new_entity(
        bronze,
        silver,
        govt_id_column="school_id_govt",
        output_column="is_new_school",
        context=context,
    )


def enrich_with_current_values(
    incoming: sql.DataFrame,
    current: sql.DataFrame,
    govt_id_column: str,
    giga_id_column: str,
    is_new_column: str,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    """Fill incoming gaps from the current row while preserving its Giga identifier."""
    logger = get_context_with_fallback_logger(context)
    logger.info("Enriching incoming rows with current values...")

    current_columns = [
        column
        for column in incoming.columns
        if column in current.columns and column not in {govt_id_column, is_new_column}
    ]
    current_selected = current.select(
        f.col(govt_id_column).alias("_current_match_id"),
        *[f.col(column).alias(f"_current_{column}") for column in current_columns],
    )
    joined = incoming.alias("incoming").join(
        current_selected,
        f.col(f"incoming.{govt_id_column}") == f.col("_current_match_id"),
        "left",
    )

    selections = []
    for column in incoming.columns:
        if column == giga_id_column and column in current_columns:
            selections.append(
                f.coalesce(
                    f.col(f"_current_{column}"),
                    f.col(f"incoming.{column}"),
                ).alias(column)
            )
        elif column in current_columns:
            selections.append(
                f.coalesce(
                    f.col(f"incoming.{column}"),
                    f.col(f"_current_{column}"),
                ).alias(column)
            )
        else:
            selections.append(f.col(f"incoming.{column}").alias(column))
    return joined.select(*selections)


def enrich_with_silver_values(
    bronze: sql.DataFrame,
    silver: sql.DataFrame,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    """Fill gaps in bronze with silver values for existing schools.

    Legacy school-geolocation wrapper: still keyed on ``school_id_*``. Facility
    bronze/DQ call ``enrich_with_current_values`` with facility column names.
    """
    return enrich_with_current_values(
        bronze,
        silver,
        govt_id_column="school_id_govt",
        giga_id_column="school_id_giga",
        is_new_column="is_new_school",
        context=context,
    )
