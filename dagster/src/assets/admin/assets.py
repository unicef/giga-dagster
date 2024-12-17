from dagster_pyspark import PySparkResource
from delta import DeltaTable
from pydantic import conint
from pyspark.sql import SparkSession
from src.constants.constants_class import constants
from src.settings import settings
from src.utils.adls import ADLSFileClient

from azure.core.exceptions import ResourceNotFoundError
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


@asset
def admin__create_lakehouse_local(
    context: OpExecutionContext, adls_file_client: ADLSFileClient
):
    lakehouse_username = settings.LAKEHOUSE_USERNAME
    if not lakehouse_username:
        raise ValueError("LAKEHOUSE_USERNAME is not set.")

    # For now, we only copy the raw schemas as that is the only folder needed to start the lakehouse.
    # After the lakehouse is created, the user can use ADLS to copy other folders as needed.

    source_raw_schema = constants.raw_schema_folder_source
    if not adls_file_client.folder_exists(source_raw_schema):
        raise ResourceNotFoundError(
            f"The folder {source_raw_schema} does not exist in {settings.AZURE_BLOB_CONTAINER_NAME}"
        )

    target_raw_schema = constants.raw_schema_folder
    if adls_file_client.folder_exists(target_raw_schema):
        context.log.info(
            f"The folder {target_raw_schema} already exists! Nothing to do."
        )
        return
    else:
        context.log.info(f"The folder {target_raw_schema} does not exist. Creating...")

    context.log.info(f"Copying folder {source_raw_schema} to {target_raw_schema}...")
    adls_file_client.copy_folder(source_raw_schema, target_raw_schema)
    context.log.info(
        f"Successfully copied folder {source_raw_schema} to {target_raw_schema}. Done!"
    )
