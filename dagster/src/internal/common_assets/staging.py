from delta import DeltaTable
from models.approval_requests import ApprovalRequest
from pyspark import sql
from pyspark.sql import SparkSession
from sqlalchemy import update

from dagster import OpExecutionContext
from src.constants import DataTier
from src.spark.transform_functions import add_missing_columns
from src.utils.adls import ADLSFileClient
from src.utils.datahub.emit_lineage import emit_lineage_base
from src.utils.db import get_db_context
from src.utils.delta import (
    build_deduped_merge_query,
    create_delta_table,
    create_schema,
    execute_query_with_error_handler,
)
from src.utils.op_config import FileConfig
from src.utils.schema import (
    construct_full_table_name,
    construct_schema_name_for_tier,
    get_primary_key,
    get_schema_columns,
)
from src.utils.spark import compute_row_hash, transform_types


def get_files_for_review(
    adls_file_client: ADLSFileClient,
    config: FileConfig,
    skip_current_file: bool = False,
):
    files_for_review = []
    for file_info in adls_file_client.list_paths(
        str(config.filepath_object.parent), recursive=False
    ):
        if skip_current_file and file_info.name == config.filepath:
            continue
        files_for_review.append(file_info)

    files_for_review = sorted(files_for_review, key=lambda p: p.last_modified)
    return files_for_review


def staging_step(
    context: OpExecutionContext,
    config: FileConfig,
    adls_file_client: ADLSFileClient,
    spark: SparkSession,
    upstream_df: sql.DataFrame,
):
    schema_name = config.metastore_schema
    country_code = config.country_code
    schema_columns = get_schema_columns(spark, schema_name)
    primary_key = get_primary_key(spark, schema_name)
    silver_tier_schema_name = construct_schema_name_for_tier(
        schema_name,
        DataTier.SILVER,
    )
    staging_tier_schema_name = construct_schema_name_for_tier(
        schema_name,
        DataTier.STAGING,
    )
    silver_table_name = construct_full_table_name(silver_tier_schema_name, country_code)
    staging_table_name = construct_full_table_name(
        staging_tier_schema_name,
        country_code,
    )

    # If silver table exists and no staging table exists, clone it to staging
    # If silver table exists and staging table exists, merge files for review to existing staging table
    # If silver table does not exist, merge files for review into one spark dataframe
    if spark.catalog.tableExists(silver_table_name):
        if not spark.catalog.tableExists(staging_table_name):
            # Clone silver table to staging
            silver = DeltaTable.forName(spark, silver_table_name).alias("silver").toDF()
            create_schema(spark, staging_tier_schema_name)
            create_delta_table(
                spark,
                staging_tier_schema_name,
                country_code,
                schema_columns,
                context,
                if_not_exists=True,
            )
            silver.write.format("delta").mode("append").saveAsTable(staging_table_name)

        # Load new table (silver clone in staging) as a deltatable
        staging_dt = DeltaTable.forName(spark, staging_table_name)
        staging = staging_dt.alias("staging").toDF()

        files_for_review = get_files_for_review(
            adls_file_client,
            config,
        )
        context.log.info(f"{len(files_for_review)=}")

        # Merge each pending file for the same country
        for file_info in files_for_review:
            existing_file = adls_file_client.download_csv_as_spark_dataframe(
                file_info.name,
                spark,
            )
            existing_file = add_missing_columns(existing_file, schema_columns)
            existing_file = transform_types(existing_file, schema_name, context)
            existing_file = compute_row_hash(existing_file)
            staging_dt = DeltaTable.forName(spark, staging_table_name)
            update_columns = [c.name for c in schema_columns if c.name != primary_key]
            query = build_deduped_merge_query(
                staging_dt,
                existing_file,
                primary_key,
                update_columns,
            )

            if query is not None:
                execute_query_with_error_handler(
                    spark,
                    query,
                    staging_tier_schema_name,
                    country_code,
                    context,
                )
            staging = staging_dt.toDF()
    else:
        staging = upstream_df
        staging = transform_types(staging, schema_name, context)
        files_for_review = get_files_for_review(
            adls_file_client, config, skip_current_file=True
        )
        context.log.info(f"{len(files_for_review)=}")

        create_schema(spark, staging_tier_schema_name)
        create_delta_table(
            spark,
            staging_tier_schema_name,
            country_code,
            schema_columns,
            context,
            if_not_exists=True,
        )
        # If no existing silver table, just merge the spark dataframes
        for file_info in files_for_review:
            existing_file = adls_file_client.download_csv_as_spark_dataframe(
                file_info.name,
                spark,
            )
            existing_file = add_missing_columns(existing_file, schema_columns)
            existing_file = transform_types(existing_file, schema_name, context)
            context.log.info(f"{existing_file.count()=}")
            staging = staging.union(existing_file)
            context.log.info(f"{staging.count()=}")

        staging = transform_types(staging, schema_name, context)
        staging = compute_row_hash(staging)
        context.log.info(f"Full {staging.count()=}")
        staging.write.format("delta").mode("append").saveAsTable(staging_table_name)

    formatted_dataset = f"School {config.dataset_type.capitalize()}"
    with get_db_context() as db:
        db.execute(
            update(ApprovalRequest)
            .where(
                ApprovalRequest.country == country_code,
                ApprovalRequest.dataset == formatted_dataset,
            )
            .values(enabled=True),
        )
        db.commit()

    upstream_filepaths = [file_info.get("name") for file_info in files_for_review]
    emit_lineage_base(
        upstream_datasets=upstream_filepaths,
        dataset_urn=config.datahub_destination_dataset_urn,
        context=context,
    )

    return staging
