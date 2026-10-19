from dagster_pyspark import PySparkResource
from pyspark.sql import SparkSession
from src.assets.analytics_tables import assets as analytics_assets

from dagster import AssetsDefinition, OpExecutionContext, asset

MIN_FILE_SIZE_BYTES = 128 * 1024 * 1024
VACUUM_RETAIN_HOURS = 168

INCREMENTAL_TABLES = sorted(
    d.key.path[-1]
    for d in vars(analytics_assets).values()
    if isinstance(d, AssetsDefinition) and d.key.path[0] == "incremental"
)


@asset(key_prefix=["maintenance"], group_name="maintenance", compute_kind="spark")
def optimize_incremental_tables(
    context: OpExecutionContext, spark: PySparkResource
) -> None:
    s: SparkSession = spark.spark_session
    s.conf.set("spark.databricks.delta.optimize.minFileSize", MIN_FILE_SIZE_BYTES)

    failed: list[str] = []
    for name in INCREMENTAL_TABLES:
        full_name = f"{analytics_assets.DELTA_LAKE_SCHEMA}.{name}"
        try:
            context.log.info(f"OPTIMIZE {full_name}")
            s.sql(f"OPTIMIZE {full_name}").collect()
            context.log.info(f"VACUUM {full_name}")
            s.sql(f"VACUUM {full_name} RETAIN {VACUUM_RETAIN_HOURS} HOURS").collect()
        except Exception as exc:
            context.log.warning(f"Maintenance failed for {full_name}: {exc}")
            failed.append(name)

    if failed:
        raise Exception(f"Maintenance failed for tables: {', '.join(failed)}")
