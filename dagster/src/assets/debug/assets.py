import json
from datetime import datetime

import pandas as pd
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
    # spark: PySparkResource,
):
    from src.internal.connectivity_queries import get_rt_schools

    rt_data = get_rt_schools()
    df = pd.DataFrame.from_records(rt_data)

    # connectivity = connectivity_rt_dataset(spark.spark_session)
    # timestamps = [
    #     r["connectivity_rt_ingestion_timestamp"]
    #     for r in connectivity.select("connectivity_rt_ingestion_timestamp").collect()
    # ]

    def serialize(val):
        if isinstance(val, pd.Timestamp | datetime):
            return val.isoformat()
        return val

    ret = (
        df[["connectivity_rt_ingestion_timestamp"]]
        .applymap(serialize)
        .to_dict(orient="records")
    )

    return Output(
        None,
        metadata={
            "preview": MetadataValue.json(ret),
            "output": json.dumps(ret, indent=2),
            "row_count": len(df),
        },
    )
