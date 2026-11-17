import re
from functools import reduce

from dagster_pyspark import PySparkResource
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import StringType
from src.utils.sentry import capture_op_exceptions
from src.utils.spark import transform_types

from dagster import OpExecutionContext, asset

from .assets import QUERIES_ROOT

SOURCE_SCHEMA = "school_master"
TARGET_SCHEMA = "custom_dataset"
TARGET_TABLE = "school_master_all"
CONSUMER_SQL = QUERIES_ROOT / "daily" / "all_school_master.sql"
CONSUMER_ALIAS_PATTERN = re.compile(r"\bmaster\.(\w+)")


def _required_columns() -> set[str]:
    return set(CONSUMER_ALIAS_PATTERN.findall(CONSUMER_SQL.read_text()))


def _read_country(
    s: SparkSession, table_name: str, context: OpExecutionContext
) -> sql.DataFrame:
    df = s.table(f"{SOURCE_SCHEMA}.{table_name}")
    df = transform_types(df, SOURCE_SCHEMA, context)
    return df.withColumn("country", f.lit(table_name.upper()))


@asset(key_prefix=["daily"], group_name="daily", compute_kind="spark")
@capture_op_exceptions
def school_master_all(context: OpExecutionContext, spark: PySparkResource) -> None:
    s: SparkSession = spark.spark_session

    table_names = sorted(
        t.name for t in s.catalog.listTables(SOURCE_SCHEMA) if not t.isTemporary
    )
    context.log.info(f"combining {len(table_names)} {SOURCE_SCHEMA} tables")

    dfs = [_read_country(s, name, context) for name in table_names]
    combined = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)

    missing = sorted(_required_columns() - set(combined.columns))
    if missing:
        context.log.warning(
            f"columns used by {CONSUMER_SQL.name} absent in all countries: {missing}"
        )
        combined = combined.withColumns(
            {name: f.lit(None).cast(StringType()) for name in missing}
        )

    s.sql(f"CREATE SCHEMA IF NOT EXISTS `{TARGET_SCHEMA}`")
    (
        combined.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{TARGET_SCHEMA}.{TARGET_TABLE}")
    )
    context.log.info(f"wrote {TARGET_SCHEMA}.{TARGET_TABLE}")
