from datetime import datetime
from pathlib import Path

import pandas as pd
from dagster_pyspark import PySparkResource
from delta import DeltaTable
from models.file_upload import FileUpload
from pyspark.sql import (
    SparkSession,
)
from pyspark.sql.types import StructType
from sqlalchemy import select
from src.constants import constants
from src.spark.transform_functions import (
    add_missing_columns,
)
from src.utils.adls import (
    ADLSFileClient,
)
from src.utils.db.primary import get_db_context
from src.utils.filename import deconstruct_school_master_filename_components
from src.utils.schema import (
    construct_full_table_name,
    get_schema_columns,
)

from dagster import OpExecutionContext, asset

DATASET_TYPE = "geolocation"
DOMAIN = "school"
DOMAIN_DATASET_TYPE = f"{DOMAIN}-{DATASET_TYPE}"
METASTORE_SCHEMA = f"{DOMAIN}_{DATASET_TYPE}"


@asset
def backfill_school_geolocation_metadata(
    context: OpExecutionContext,
    spark: PySparkResource,
    adls_file_client: ADLSFileClient,
):
    s: SparkSession = spark.spark_session

    source_directory = f"{constants.UPLOAD_PATH_PREFIX}/{DOMAIN_DATASET_TYPE}"

    all_metadata_df = pd.DataFrame()

    for file_data in adls_file_client.list_paths_generator(
        source_directory, recursive=True
    ):
        if file_data.is_directory:
            continue

        adls_filepath = file_data.name
        path = Path(adls_filepath)
        try:
            filename_components = deconstruct_school_master_filename_components(
                adls_filepath
            )
        except Exception as e:
            context.log.error(f"Failed to deconstruct filename: {adls_filepath}: {e}")
            continue
        else:
            country_code = filename_components.country_code
            properties = adls_file_client.get_file_metadata(filepath=adls_filepath)
            metadata = properties.metadata
            file_size_bytes = properties.size

        context.log.info("Get upload details")
        country_code = country_code
        schema_name = METASTORE_SCHEMA
        file_name = path.name
        giga_sync_id = file_name.split("_")[0]
        giga_sync_uploaded_at = datetime.strptime(
            file_name.split(".")[0].split("_")[-1], "%Y%m%d-%H%M%S"
        )

        with get_db_context() as db:
            file_upload = db.scalar(
                select(FileUpload).where(FileUpload.id == giga_sync_id),
            )
            if file_upload is None:
                context.log.error(
                    f"Database entry for FileUpload with id {giga_sync_id} and country {country_code} was not found"
                )
                continue

        upload_details = {
            "giga_sync_id": giga_sync_id,
            "country_code": country_code,
            "giga_sync_uploaded_at": giga_sync_uploaded_at,
            "schema_name": schema_name,
            "raw_file_path": adls_filepath,
            "file_size_bytes": file_size_bytes,
        }

        context.log.info(f"Creating metadata for {country_code}")

        context.log.info(
            f"The type of the raw_file is: {type(upload_details['raw_file_path'])}"
        )

        context.log.info("Create upload details dataframe")
        df = pd.DataFrame([upload_details])

        context.log.info("Create giga sync metadata dataframe")
        metadata_df = pd.DataFrame([metadata])

        context.log.info("Combine dataframes")
        metadata_df = pd.concat([df, metadata_df], axis="columns")
        metadata_df["created_at"] = pd.Timestamp.now()

        all_metadata_df = pd.concat([all_metadata_df, metadata_df], axis="rows")

    context.log.info("Create spark dataframe")
    all_metadata_df = s.createDataFrame(all_metadata_df)

    table_columns = get_schema_columns(s, "school_geolocation_metadata")
    table_name = "school_geolocation_metadata"
    table_schema_name = "pipeline_tables"

    context.log.info("Create the schema and table if they do not exist")
    all_metadata_df = add_missing_columns(all_metadata_df, table_columns)
    all_metadata_df = all_metadata_df.select(*StructType(table_columns).fieldNames())

    context.log.info("Upsert the metadata from giga sync into the table")
    s.catalog.refreshTable(construct_full_table_name(table_schema_name, table_name))
    current_metadata_table = DeltaTable.forName(
        s, construct_full_table_name(table_schema_name, table_name)
    )

    (
        current_metadata_table.alias("metadata_current")
        .merge(
            all_metadata_df.alias("metadata_updates"),
            "metadata_current.giga_sync_id = metadata_updates.giga_sync_id",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    context.log.info("Upsert operation completed")
