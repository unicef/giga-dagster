from dagster_pyspark import PySparkResource
from delta import DeltaTable
from pydantic import conint
from pyspark.sql import SparkSession

from dagster import (
    Config,
    DagsterRunStatus,
    MetadataValue,
    OpExecutionContext,
    Output,
    RunsFilter,
    asset,
)


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


class RollbackTableConfig(Config):
    schema_name: str
    table_name: str
    version: conint(ge=0)


@asset
def admin__rollback_table_version(
    _: OpExecutionContext, config: RollbackTableConfig, spark: PySparkResource
):
    s: SparkSession = spark.spark_session
    dt = DeltaTable.forName(s, f"{config.schema_name}.{config.table_name}")
    result = dt.restoreToVersion(config.version)

    return Output(
        None,
        metadata={
            "result": MetadataValue.md(result.toPandas().to_markdown()),
        },
    )
