import pandas as pd
from dagster_pyspark import PySparkResource
from pyspark.sql import SparkSession
from sqlalchemy import text

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
    from src.utils.db.mlab import get_db_context

    with get_db_context() as db:
        res = (
            db.execute(
                text("""
        SELECT
            DISTINCT mlab.school_id school_id_govt,
            (min(mlab."timestamp") over (partition by mlab.school_id))::DATE mlab_created_date,
            client_info::JSON ->> 'Country' country_code,
            'mlab' source
        FROM public.measurements mlab
        LIMIT 10
        """)
            )
            .mappings()
            .all()
        )

    res = pd.DataFrame.from_records(res)
    return Output(None, metadata={"mlab_schools": MetadataValue.md(res.to_markdown())})


@asset
def debug__test_proco_db_connection(_: OpExecutionContext):
    from src.utils.db.proco import get_db_context

    with get_db_context() as db:
        rt_schools = (
            db.execute(
                text("""
                SELECT
                    DISTINCT sch.giga_id_school school_id_giga,
                    sch.external_id school_id_govt,
                    (min(stat.created) over (partition by stat.school_id)) connectivity_RT_ingestion_timestamp,
                    c.code country_code,
                    c.name country
                FROM connection_statistics_schooldailystatus stat
                LEFT JOIN schools_school sch ON sch.id = stat.school_id
                LEFT JOIN locations_country c ON c.id = sch.country_id
                LIMIT 10
                """)
            )
            .mappings()
            .all()
        )

        giga_meter_schools = (
            db.execute(
                text("""
                SELECT
                    DISTINCT dca.giga_id_school school_id_giga,
                    school_id school_id_govt,
                    'daily_checkapp' source,
                FROM dailycheckapp_measurements dca
                WHERE dca.giga_id_school !=''
                LIMIT 10
                """)
            )
            .mappings()
            .all()
        )

    rt_schools = pd.DataFrame.from_records(rt_schools)
    giga_meter_schools = pd.DataFrame.from_records(giga_meter_schools)

    return Output(
        None,
        metadata={
            "giga_meter_schools": MetadataValue.md(giga_meter_schools.to_markdown()),
            "rt_schools": MetadataValue.md(rt_schools.to_markdown()),
        },
    )
