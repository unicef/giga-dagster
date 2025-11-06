from dagster_pyspark import PySparkResource
from httpx import AsyncClient
from pydantic import Field
from pyspark.sql import SparkSession
from src.settings import settings
from src.utils.sentry import capture_op_exceptions

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
@capture_op_exceptions
def debug__drop_schema(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: DropSchemaConfig,
):
    s: SparkSession = spark.spark_session
    s.sql(f"DROP SCHEMA IF EXISTS {config.schema_name} CASCADE")
    context.log.info(f"Dropped schema {config.schema_name}")


@asset
@capture_op_exceptions
def debug__drop_table(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: DropTableConfig,
):
    s: SparkSession = spark.spark_session
    s.sql(f"DROP TABLE IF EXISTS {config.schema_name}.{config.table_name}")
    context.log.info(f"Dropped table {config.schema_name}.{config.table_name}")


@asset
@capture_op_exceptions
def debug__test_mlab_db_connection(
    _: OpExecutionContext, config: ExternalDbQueryConfig
):
    from src.internal.connectivity_queries import get_mlab_schools

    res = get_mlab_schools(config.country_code, is_test=True)
    return Output(None, metadata={"mlab_schools": MetadataValue.md(res.to_markdown())})


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
@capture_op_exceptions
def debug__test_connectivity_merge(
    _: OpExecutionContext, config: ExternalDbQueryConfig
):
    import numpy as np
    from src.internal.connectivity_queries import (
        get_giga_meter_schools,
        get_mlab_schools,
        get_rt_schools,
    )

    rt_schools = get_rt_schools(config.country_code, is_test=False)
    giga_meter_schools = get_giga_meter_schools(is_test=False)
    mlab_schs = get_mlab_schools(config.country_code, is_test=False)

    # merge datasets (here we should merge schools from QoS too as part of the pipeline )
    rt_schools = rt_schools.merge(
        mlab_schs, on=["school_id_govt", "country_code"], how="left"
    ).merge(giga_meter_schools, on="school_id_giga", how="left", suffixes=("", "_pcdc"))

    # determine the data source(s)
    rt_schools["source"] = (
        rt_schools[["source_pcdc", "source"]]
        .fillna("")
        .apply(lambda r: ", ".join(r), axis="columns")
        .str.strip(" ")
        .str.strip(",")
    )

    # if we merge schools from QoS we probably do not need this line specific to Brazil
    rt_schools["connectivity_rt_datasource"] = np.where(
        (rt_schools["source"] == "") & (rt_schools["country"] == "Brazil"),
        "nic_br",
        rt_schools["source"],
    )

    # retain only necessay columns
    realtime_columns = [
        "school_id_giga",
        "country",
        "school_id_govt",
        "connectivity_rt_ingestion_timestamp",
        "connectivity_rt_datasource",
    ]
    rt_schools_original = rt_schools.loc[rt_schools["source"] != "", realtime_columns]
    rt_schools_original["connectivity_rt"] = "Yes"

    rt_schools_refined = rt_schools.loc[
        rt_schools["connectivity_rt_datasource"] != "", realtime_columns
    ]
    rt_schools_refined["connectivity_rt"] = "Yes"

    rt_schools = rt_schools.loc[:, realtime_columns]
    rt_schools["connectivity_rt"] = "Yes"

    describe_stats_original = rt_schools_original.describe().to_markdown()
    summary_stats_original = (
        rt_schools_original.groupby(["connectivity_rt_datasource", "country"])
        .agg(
            total_rows=("connectivity_rt_ingestion_timestamp", "count"),
            distinct_schools=("school_id_giga", "nunique"),
        )
        .reset_index()
        .to_markdown()
    )

    describe_stats_refined = rt_schools_original.describe().to_markdown()
    summary_stats_refined = (
        rt_schools_original.groupby(["connectivity_rt_datasource", "country"])
        .agg(
            total_rows=("connectivity_rt_ingestion_timestamp", "count"),
            distinct_schools=("school_id_giga", "nunique"),
        )
        .reset_index()
        .to_markdown()
    )

    summary_stats = (
        rt_schools.groupby(["connectivity_rt_datasource", "country"])
        .agg(
            total_rows=("connectivity_rt_ingestion_timestamp", "count"),
            distinct_schools=("school_id_giga", "nunique"),
        )
        .reset_index()
        .to_markdown()
    )

    return Output(
        None,
        metadata={
            # "giga_meter_schools": MetadataValue.md(giga_meter_schools.to_markdown()),
            # "rt_schools": MetadataValue.md(rt_schools.to_markdown()),
            "rt_schools_original_describe": MetadataValue.md(describe_stats_original),
            "rt_schools_original_summary": MetadataValue.md(summary_stats_original),
            "rt_schools_refined_describe": MetadataValue.md(describe_stats_refined),
            "rt_schools_refined_summary": MetadataValue.md(summary_stats_refined),
            "rt_schools_summary": MetadataValue.md(summary_stats),
        },
    )


# @asset
# def debug__test_connectivity_merge(
#     _: OpExecutionContext,
#     spark: PySparkResource,
#     # config: ExternalDbQueryConfig,
# ):
#     # from country_converter import CountryConverter
#     from src.spark.transform_functions import connectivity_rt_dataset

#     # coco = CountryConverter()
#     # country_code_2 = coco.convert("BRA", to="ISO2")
#     connectivity = connectivity_rt_dataset(spark.spark_session, "BR", is_test=False)

#     stats = (
#         connectivity.groupBy("country")
#         .agg(f.count("*").alias("count"))
#         .orderBy("count", ascending=False)
#     )
#     stats_dict = stats.rdd.collectAsMap()

#     return Output(
#         None,
#         metadata={
#             "preview": get_table_preview(connectivity),
#             "stats_preview": get_table_preview(stats),
#             "row_count": connectivity.count(),
#             "stats_dict": stats_dict,
#         },
#     )


@asset
@capture_op_exceptions
async def debug__send_test_email(
    context: OpExecutionContext, config: GenericEmailRequestConfig
):
    async with AsyncClient(base_url=settings.GIGASYNC_API_URL) as client:
        res = await client.post(
            "/api/email/send-email",
            headers={"Authorization": f"Bearer {settings.EMAIL_RENDERER_BEARER_TOKEN}"},
            json=config.model_dump(),
        )
        if res.is_error:
            context.log.error(res.json())
            res.raise_for_status()

    return Output(None, metadata=config.model_dump())
