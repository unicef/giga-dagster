from dagster_pyspark import PySparkResource
from datahub.specific.dataset import DatasetPatchBuilder
from delta import DeltaTable
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from src.data_quality_checks.utils import (
    aggregate_report_json,
    aggregate_report_spark_df,
    row_level_checks,
)
from src.utils.datahub.create_validation_tab import (
    datahub_emit_assertions_with_exception_catcher,
)
from src.utils.datahub.emit_dataset_metadata import (
    datahub_emit_metadata_with_exception_catcher,
)
from src.utils.datahub.emitter import get_rest_emitter
from src.utils.delta import get_change_operation_counts
from src.utils.logger import ContextLoggerWithLoguruFallback
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig
from src.utils.schema import get_schema_columns_datahub

from dagster import OpExecutionContext, Output, asset


@asset
def adhoc__standalone_master_data_quality_checks(
    context: OpExecutionContext,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[None]:
    logger = ContextLoggerWithLoguruFallback(context)
    s: SparkSession = spark.spark_session
    dt = DeltaTable.forName(s, f"school_master.{config.country_code}")
    master = dt.toDF()

    dq_checked = logger.passthrough(
        row_level_checks(
            df=master,
            dataset_type=config.dataset_type,
            _country_code_iso3=config.country_code,
            context=context,
        ),
        "Row level checks completed",
    )
    dq_summary = aggregate_report_json(
        df_aggregated=aggregate_report_spark_df(s, dq_checked),
        df_bronze=dq_checked,
        df_data_quality_checks=dq_checked,
    )

    latest_version = (dt.history().orderBy(f.col("version").desc()).first()).version
    if latest_version is None:
        latest_version = 0

    cdf = (
        s.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", latest_version)
        .table(f"school_master.{config.country_code}")
    )

    if cdf.count() == 0:
        context.log.warning("No changes to master, skipping checks.")
        return Output(None)

    counts = get_change_operation_counts(cdf)

    with get_rest_emitter() as emitter:
        context.log.info(f"{config.datahub_destination_dataset_urn=}")

        for patch_mcp in (
            DatasetPatchBuilder(config.datahub_destination_dataset_urn)
            .add_custom_properties(
                {
                    "Dataset Version": str(latest_version),
                    "Row Count": f"{master.count():,}",
                    "Rows Added": f'{counts["added"]:,}',
                    "Rows Updated": f'{counts["modified"]:,}',
                    "Rows Deleted": f'{counts["deleted"]:,}',
                }
            )
            .build()
        ):
            try:
                emitter.emit(patch_mcp, lambda e, s: context.log.info(f"{e=}\n{s=}"))
            except Exception as e:
                context.log.error(str(e))

    schema_reference = get_schema_columns_datahub(s, config.metastore_schema)
    datahub_emit_metadata_with_exception_catcher(
        context=context, config=config, spark=spark, schema_reference=schema_reference
    )
    datahub_emit_assertions_with_exception_catcher(
        context=context, dq_summary_statistics=dq_summary
    )

    return Output(
        None,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(dq_checked),
        },
    )
