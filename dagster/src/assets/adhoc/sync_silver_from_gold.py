from src.resources import ResourceKey
from src.utils.op_config import DatasetConfig
from src.utils.sentry import capture_op_exceptions
from src.utils.silver_sync import sync_silver_from_gold

from dagster import AssetIn, OpExecutionContext, Output, asset


@asset(
    required_resource_keys={ResourceKey.SPARK.value},
)
@capture_op_exceptions
def adhoc__sync_silver_geolocation_from_gold(
    context: OpExecutionContext,
    config: DatasetConfig,
) -> Output[None]:
    return sync_silver_from_gold(
        context, context.resources.spark, config, "school_geolocation"
    )  # noqa: F821


@asset(
    ins={"start_after": AssetIn("adhoc__sync_silver_geolocation_from_gold")},
    required_resource_keys={ResourceKey.SPARK.value},
)
@capture_op_exceptions
def adhoc__sync_silver_coverage_from_gold(
    context: OpExecutionContext,
    config: DatasetConfig,
    start_after,
) -> Output[None]:
    return sync_silver_from_gold(
        context, context.resources.spark, config, "school_coverage"
    )  # noqa: F821
