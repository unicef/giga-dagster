from dagster_pyspark import PySparkResource
from pyspark.sql import SparkSession

from dagster import Config, DagsterRunStatus, OpExecutionContext, RunsFilter, asset


@asset
def admin__terminate_all_runs(context: OpExecutionContext):
    runs = [
        r
        for r in context.instance.get_runs(
            filters=RunsFilter(
                statuses=[
                    DagsterRunStatus.STARTED,
                    DagsterRunStatus.STARTING,
                    DagsterRunStatus.QUEUED,
                    DagsterRunStatus.NOT_STARTED,
                ],
            ),
        )
        if r.run_id != context.run_id or not r.job_name.startswith("admin__")
    ]

    for run in runs:
        context.instance.report_run_canceled(run)
        context.log.info(f"Terminated run {run.job_name} ({run.run_id})")

    context.log.info(f"Terminated {len(runs)} run(s)")


class DropSchemaConfig(Config):
    schema_name: str


class DropTableConfig(DropSchemaConfig):
    table_name: str


@asset
def admin__drop_schema(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: DropSchemaConfig,
):
    s: SparkSession = spark.spark_session
    s.sql(f"DROP SCHEMA IF EXISTS {config.schema_name} CASCADE")
    context.log.info(f"Dropped schema {config.schema_name}")


@asset
def admin__drop_table(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: DropTableConfig,
):
    s: SparkSession = spark.spark_session
    s.sql(f"DROP TABLE IF EXISTS {config.schema_name}.{config.table_name}")
    context.log.info(f"Dropped table {config.schema_name}.{config.table_name}")
