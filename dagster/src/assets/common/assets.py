from dagster_pyspark import PySparkResource
from delta.tables import DeltaTable
from pyspark import sql
from src.resources import ResourceKey
from src.settings import settings
from src.utils.adls import ADLSFileClient, get_filepath, get_output_filepath
from src.utils.datahub.emit_dataset_metadata import emit_metadata_to_datahub
from src.utils.op_config import FileConfig
from src.utils.schema import get_schema_columns_datahub

from dagster import AssetOut, OpExecutionContext, Output, asset, multi_asset


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def manual_review_passed_rows(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
    config: FileConfig,
) -> sql.DataFrame:
    # dataset_type = context.get_step_execution_context().op_config["dataset_type"]
    # filepath = context.run_tags["dagster/run_key"].split("/")[-1]
    # table_name = filepath.split("/").split("_")[1]
    # table_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{get_filepath(filepath, dataset_type, 'manual_review_passed_rows').split('/')[:-1]}/{table_name}",

    df = adls_file_client.download_csv_as_pandas_dataframe(
        context.run_tags["dagster/run_key"], spark.spark_session
    )

    try:
        schema_reference = get_schema_columns_datahub(
            spark.spark_session, config.metastore_schema
        )
        emit_metadata_to_datahub(
            context,
            schema_reference=schema_reference,
            country_code=config.filename_components.country_code,
            dataset_urn=config.datahub_destination_dataset_urn,
        )
    except Exception as error:
        context.log.info(f"Error on Datahub Emit Metadata: {error}")

    yield Output(df, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def manual_review_failed_rows(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
    config: FileConfig,
) -> sql.DataFrame:
    # dataset_type = context.get_step_execution_context().op_config["dataset_type"]
    # filepath = context.run_tags["dagster/run_key"].split("/")[-1]
    # table_name = filepath.split("/").split("_")[1]
    # table_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{get_filepath(filepath, dataset_type, 'manual_review_failed_rows').split('/')[:-1]}/{table_name}",

    df = adls_file_client.download_csv_as_pandas_dataframe(
        context.run_tags["dagster/run_key"]
    )

    try:
        schema_reference = get_schema_columns_datahub(
            spark.spark_session, config.metastore_schema
        )
        emit_metadata_to_datahub(
            context,
            schema_reference=schema_reference,
            country_code=config.filename_components.country_code,
            dataset_urn=config.datahub_destination_dataset_urn,
        )
    except Exception as error:
        context.log.info(f"Error on Datahub Emit Metadata: {error}")

    yield Output(df, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def silver(
    context: OpExecutionContext,
    manual_review_passed_rows: sql.DataFrame,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
    config: FileConfig,
) -> sql.DataFrame:
    dataset_type = config["dataset_type"]
    filepath = context.run_tags["dagster/run_key"].split("/")[-1]
    silver_table_name = filepath.split("/")[-1].split("_")[1]
    silver_table_path = (
        f"{settings.AZURE_BLOB_CONNECTION_URI}/{get_filepath(filepath, dataset_type, 'silver').split('/')[:-1]}/{silver_table_name}",
    )

    if DeltaTable.isDeltaTable(spark.spark_session, silver_table_path):
        silver = adls_file_client.download_delta_table_as_delta_table(
            silver_table_path, spark.spark_session
        )

        silver = (
            silver.alias("source")
            .merge(
                manual_review_passed_rows.alias("target"),
                "source.school_id_giga = target.school_id_giga",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    try:
        schema_reference = get_schema_columns_datahub(
            spark.spark_session, config.metastore_schema
        )
        emit_metadata_to_datahub(
            context,
            schema_reference=schema_reference,
            country_code=config.filename_components.country_code,
            dataset_urn=config.datahub_destination_dataset_urn,
        )
    except Exception as error:
        context.log.info(f"Error on Datahub Emit Metadata: {error}")

    yield Output(silver, metadata={"filepath": get_output_filepath(context)})


@multi_asset(
    outs={
        "master": AssetOut(
            is_required=True, io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value
        ),
        "reference": AssetOut(
            is_required=True, io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value
        ),
    }
)
def gold(
    context: OpExecutionContext,
    config: FileConfig,
    silver: sql.DataFrame,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
) -> sql.DataFrame:
    dataset_type = config["dataset_type"]
    filepath = context.run_tags["dagster/run_key"].split("/")[-1]
    gold_table_name = filepath.split("/")[-1].split("_")[1]
    gold_master_table_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{get_filepath(filepath, dataset_type, 'gold_master').split('/')[:-1]}/{gold_table_name}"
    gold_reference_table_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{get_filepath(filepath, dataset_type, 'gold_reference').split('/')[:-1]}/{gold_table_name}"

    master_columns = [
        "cellular_coverage_availability",
        "cellular_coverage_type",
        "microwave_node_distance",
        "schools_within_1km",
        "schools_within_2km",
        "schools_within_3km",
        "nearest_NR_distance",
        "nearest_LTE_distance",
        "nearest_UMTS_distance",
        "nearest_GSM_distance",
        "pop_within_1km",
        "pop_within_2km",
        "pop_within_3km",
        "connectivity_govt_collection_year",
        "connectivity_govt",
        "school_id_giga",
        "school_id_govt",
        "school_name",
        "school_establishment_year",
        "latitude",
        "longitude",
        "education_level",
        "download_speed_contracted",
        "connectivity_type_govt",
        "admin1",
        "admin1_id_giga",
        "admin2",
        "admin2_id_giga",
        "school_area_type",
        "school_funding_type",
        "num_computers",
        "num_computers_desired",
        "num_teachers",
        "num_adm_personnel",
        "num_students",
        "num_classrooms",
        "num_latrines",
        "computer_lab",
        "electricity_availability",
        "electricity_type",
        "water_availability",
        "school_data_source",
        "school_data_collection_year",
        "school_data_collection_modality",
        "connectivity_govt_ingestion_timestamp",
        "school_location_ingestion_timestamp",
        "disputed_region",
        "connectivity",
        "connectivity_RT",
        "connectivity_RT_datasource",
        "connectivity_RT_ingestion_timestamp",
    ]

    reference_columns = [
        "school_id_giga",
        "pop_within_10km"
        "nearest_school_distance"
        "schools_within_10km"
        "nearest_NR_id",
        "nearest_LTE_id",
        "nearest_UMTS_id",
        "nearest_GSM_id",
        "education_level_govt",
        "download_speed_govt",
        "school_id_govt_type",
        "school_address",
        "is_school_open",
    ]

    silver_master = silver.select(master_columns)
    silver_reference = silver.select(reference_columns)

    if DeltaTable.isDeltaTable(spark.spark_session, gold_master_table_path):
        master = adls_file_client.download_delta_table_as_delta_table(
            gold_master_table_path, spark.spark_session
        )

        master.alias("source").merge(
            silver_master.alias("target"),
            "source.school_id_giga = target.school_id_giga",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    if DeltaTable.isDeltaTable(spark.spark_session, gold_reference_table_path):
        reference = adls_file_client.download_delta_table_as_delta_table(
            gold_reference_table_path, spark.spark_session
        )

        reference.alias("source").merge(
            silver_reference.alias("target"),
            "source.school_id_giga = target.school_id_giga",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    try:
        schema_reference = get_schema_columns_datahub(
            spark.spark_session, config.metastore_schema
        )
        emit_metadata_to_datahub(
            context,
            schema_reference=schema_reference,
            country_code=config.filename_components.country_code,
            dataset_urn=config.datahub_destination_dataset_urn,
        )
    except Exception as error:
        context.log.info(f"Error on Datahub Emit Metadata: {error}")

    yield Output(
        master,
        metadata={"filepath": get_output_filepath(context, "master")},
        output_name="master",
    )
    yield Output(
        reference,
        metadata={"filepath": get_output_filepath(context, "reference")},
        output_name="reference",
    )
