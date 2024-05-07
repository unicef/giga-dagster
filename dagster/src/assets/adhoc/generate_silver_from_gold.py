from dagster_pyspark import PySparkResource
from pyspark import sql
from src.resources import ResourceKey
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig
from src.utils.schema import (
    get_schema_columns,
)

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def adhoc__generate_silver_geolocation(
    context: OpExecutionContext,
    config: FileConfig,
    spark: PySparkResource,
    adhoc__publish_master_to_gold: sql.DataFrame,
    adhoc__publish_reference_to_gold: sql.DataFrame,
) -> Output[sql.DataFrame]:
    df_one_gold = adhoc__publish_master_to_gold.join(
        adhoc__publish_reference_to_gold, on="school_id_giga", how="left"
    )

    schema_columns = get_schema_columns(spark, schema_name="school_geolocation")
    df_silver = df_one_gold.select([c.name for c in schema_columns])

    return Output(
        df_silver,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_silver),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def adhoc__generate_silver_coverage(
    context: OpExecutionContext,
    config: FileConfig,
    spark: PySparkResource,
    adhoc__publish_master_to_gold: sql.DataFrame,
    adhoc__publish_reference_to_gold: sql.DataFrame,
) -> Output[sql.DataFrame]:
    df_one_gold = adhoc__publish_master_to_gold.join(
        adhoc__publish_reference_to_gold, on="school_id_giga", how="left"
    )

    schema_columns = get_schema_columns(spark, schema_name="school_coverage")
    df_silver = df_one_gold.select([c.name for c in schema_columns])

    return Output(
        df_silver,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_silver),
        },
    )
