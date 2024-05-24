from dagster_pyspark import PySparkResource
from httpx import AsyncClient
from pydantic import Field
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from src.settings import settings
from src.utils.metadata import get_table_preview

from dagster import Config, MetadataValue, OpExecutionContext, Output, asset


class DropSchemaConfig(Config):
    schema_name: str


class DropTableConfig(DropSchemaConfig):
    table_name: str


class ExternalDbQueryConfig(Config):
    country_code: str = Field(
        ...,
        min_length=2,
        max_length=2,
        description="ISO-2 country code",
    )


class GenericEmailRequestConfig(Config):
    recipients: list[str]
    subject: str
    html_part: str | None
    text_part: str | None


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
def debug__test_mlab_db_connection(
    _: OpExecutionContext, config: ExternalDbQueryConfig
):
    from src.internal.connectivity_queries import get_mlab_schools

    res = get_mlab_schools(config.country_code, is_test=True)
    return Output(None, metadata={"mlab_schools": MetadataValue.md(res.to_markdown())})


@asset
def debug__test_proco_db_connection(
    _: OpExecutionContext, config: ExternalDbQueryConfig
):
    from src.internal.connectivity_queries import get_giga_meter_schools, get_rt_schools

    rt_schools = get_rt_schools(config.country_code, is_test=True)
    giga_meter_schools = get_giga_meter_schools(is_test=True)

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
    config: ExternalDbQueryConfig,
):
    from country_converter import CountryConverter
    from src.spark.transform_functions import connectivity_rt_dataset

    coco = CountryConverter()
    country_code_2 = coco.convert("BRA", to="ISO2")
    connectivity = connectivity_rt_dataset(
        spark.spark_session, country_code_2, is_test=False
    )

    stats = (
        connectivity.groupBy("country")
        .agg(f.count("*").alias("count"))
        .orderBy("count", ascending=False)
    )
    stats_dict = stats.rdd.collectAsMap()

    return Output(
        None,
        metadata={
            "preview": get_table_preview(connectivity),
            "stats_preview": get_table_preview(stats),
            "row_count": connectivity.count(),
            "stats_dict": stats_dict,
        },
    )


@asset
async def debug__send_test_email(
    context: OpExecutionContext, config: GenericEmailRequestConfig
):
    async with AsyncClient(base_url=settings.GIGASYNC_API_URL) as client:
        res = await client.post(
            "/api/email/send-email",
            headers={"Authorization": f"Bearer {settings.EMAIL_RENDERER_BEARER_TOKEN}"},
            json=config.dict(),
        )
        if res.is_error:
            context.log.error(res.json())
            res.raise_for_status()

    return Output(None, metadata=config.dict())
