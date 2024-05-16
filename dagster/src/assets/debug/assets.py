from dagster_pyspark import PySparkResource
from pyspark.sql import SparkSession
from sqlalchemy import select, text

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
def debug__test_mlab_db_connection(context: OpExecutionContext):
    from models.connectivity_rt import MlabSchools
    from src.schemas.connectivity_rt import MlabSchools as MlabSchoolsSchema
    from src.utils.db.mlab import get_db_context

    with get_db_context() as db:
        context.log.info(db.execute(text("SELECT 1")))
        res = db.scalars(select(MlabSchools).limit(10)).all()
        res = [MlabSchoolsSchema.from_orm(r) for r in res]

    return Output(None, metadata={"mlab_schools": MetadataValue.json(res)})


@asset
def debug__test_proco_db_connection(context: OpExecutionContext):
    from models.connectivity_rt import GigaMeterSchools, RTSchools
    from src.schemas.connectivity_rt import (
        GigaMeterSchools as GigaMeterSchoolsSchema,
        RTSchools as RTSchoolsSchema,
    )
    from src.utils.db.proco import get_db_context

    with get_db_context() as db:
        context.log.info(db.execute(text("SELECT 1")))
        res = db.scalars(select(GigaMeterSchools).limit(10)).all()
        giga_meter_schools = [GigaMeterSchoolsSchema.from_orm(r) for r in res]

        res = db.scalars(select(RTSchools).limit(10)).all()
        rt_schools = [RTSchoolsSchema.from_orm(r) for r in res]

    return Output(
        None,
        metadata={
            "giga_meter_schools": MetadataValue.json(
                [g.dict() for g in giga_meter_schools]
            ),
            "rt_schools": MetadataValue.json([r.dict() for r in rt_schools]),
        },
    )
