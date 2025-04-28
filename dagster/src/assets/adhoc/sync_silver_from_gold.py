from dagster_pyspark import PySparkResource
from src.utils.op_config import DatasetConfig
from src.utils.sentry import capture_op_exceptions
from src.utils.silver_sync import sync_silver_from_gold

from dagster import AssetIn, OpExecutionContext, Output, asset


@asset
@capture_op_exceptions
def adhoc__sync_silver_geolocation_from_gold(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: DatasetConfig,
) -> Output[None]:
    return sync_silver_from_gold(context, spark, config, "school_geolocation")


@asset(ins={"start_after": AssetIn("adhoc__sync_silver_geolocation_from_gold")})
@capture_op_exceptions
def adhoc__sync_silver_coverage_from_gold(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: DatasetConfig,
    start_after,
) -> Output[None]:
    return sync_silver_from_gold(context, spark, config, "school_coverage")
