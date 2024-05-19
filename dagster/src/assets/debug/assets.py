from dagster_pyspark import PySparkResource
from pyspark.sql import SparkSession

from dagster import Config, MetadataValue, OpExecutionContext, Output, asset


class DropSchemaConfig(Config):
    schema_name: str


class DropTableConfig(DropSchemaConfig):
    table_name: str


@asset
def debug__drop_schema(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: DropSchemaConfig,
):
    s: SparkSession = spark.spark_session
    s.sql(f"DROP SCHEMA IF EXISTS {config.schema_name} CASCADE")
    context.log.info(f"Dropped schema {config.schema_name}")


@asset
def debug__drop_table(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: DropTableConfig,
):
    s: SparkSession = spark.spark_session
    s.sql(f"DROP TABLE IF EXISTS {config.schema_name}.{config.table_name}")
    context.log.info(f"Dropped table {config.schema_name}.{config.table_name}")


@asset
def debug__test_mlab_db_connection(_: OpExecutionContext):
    from src.internal.connectivity_queries import get_mlab_schools

    res = get_mlab_schools()
    return Output(None, metadata={"mlab_schools": MetadataValue.md(res.to_markdown())})


@asset
def debug__test_proco_db_connection(_: OpExecutionContext):
    from src.internal.connectivity_queries import get_giga_meter_schools, get_rt_schools

    rt_schools = get_rt_schools()
    giga_meter_schools = get_giga_meter_schools()

    return Output(
        None,
        metadata={
            "giga_meter_schools": MetadataValue.md(giga_meter_schools.to_markdown()),
            "rt_schools": MetadataValue.md(rt_schools.to_markdown()),
        },
    )


@asset
def debug__test_connectivity_merge(
    _: OpExecutionContext,
    spark: PySparkResource,
):
    from src.spark.transform_functions import connectivity_rt_dataset

    return Output(
        None,
        metadata={
            "preview": MetadataValue.md(
                connectivity_rt_dataset(spark.spark_session)
                .toPandas()
                .head(10)
                .to_markdown()
            )
        },
    )
