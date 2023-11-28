import json
from io import BytesIO

from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql import DataFrame, SparkSession

from dagster import OpExecutionContext
from src.constants import constants
from src.settings import settings


class ADLSFileClient:
    def __init__(self):
        self.client = DataLakeServiceClient(
            account_url=f"https://{settings.AZURE_DFS_SAS_HOST}",
            credential=settings.AZURE_SAS_TOKEN,
        )
        self.adls = self.client.get_file_system_client(
            file_system=settings.AZURE_BLOB_CONTAINER_NAME
        )

    def download_adls_csv_to_spark_dataframe(self, spark: SparkSession, filepath: str):
        file_client = self.adls.get_file_client(filepath)

        with BytesIO() as buffer:
            file_client.download_file().readinto(buffer)
            buffer.seek(0)
            return spark.read.csv(buffer)

    # def upload_spark_df_to_adls_deltatable(self, filepath: str, data: pd.DataFrame):
    #     file_client = self.adls.get_file_client(filepath)

    #     with BytesIO() as buffer:
    #         data.to_csv(buffer, index=False)
    #         buffer.seek(0)
    #         file_client.upload_data(buffer.getvalue(), overwrite=True)

    def download_adls_deltatable_to_spark_dataframe(
        self, spark: SparkSession, filepath: str
    ):
        df = spark.read.format("delta").load(filepath)
        df.show()

    def upload_spark_dataframe_to_adls_deltatable(
        self, spark: SparkSession, data: DataFrame, filepath: str
    ):
        filename = filepath.split("/")[-1]
        spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
        spark.sql(
            f"""
                CREATE TABLE IF NOT EXISTS gold.{filename} (
                    giga_id_school STRING
                    school_id STRING
                    name STRING
                    education_level STRING
                    lat DOUBLE
                    lon DOUBLE
                    connectivity STRING
                    type_connectivity STRING
                    connectivity_speed LONG
                    num_students LONG
                    num_computers LONG
                    electricity STRING
                    computer_availability STRING
                    education_level_regional STRING
                    school_type STRING
                    coverage_availability STRING
                    coverage_type STRING
                    latency_connectivity STRING
                    admin1 STRING
                    admin2 STRING
                    admin3 STRING
                    admin4 STRING
                    school_region STRING
                    num_teachers LONG
                    num_classroom LONG
                    computer_lab STRING
                    water STRING
                    address STRING
                    fiber_node_distance DOUBLE
                    microwave_node_distance DOUBLE
                    nearest_school_distance DOUBLE
                    schools_within_1km LONG
                    schools_within_2km LONG
                    schools_within_3km LONG
                    schools_within_10km LONG
                    nearest_LTE_id LONG
                    nearest_LTE_distance DOUBLE
                    nearest_UMTS_id LONG
                    nearest_UMTS_distance DOUBLE
                    nearest_GSM_id LONG
                    nearest_GSM_distance DOUBLE
                    pop_within_1km LONG
                    pop_within_2km LONG
                    pop_within_3km LONG
                    pop_within_10km LONG
                )
                USING DELTA
                LOCATION f'{settings.AZURE_BLOB_CONNECTION_URI}/gold/delta-tables'
            """
        )
        data.write.format("delta").mode("overwrite").saveAsTable(f"gold.{filename}")

    def download_adls_json_to_json(self, filepath: str):
        file_client = self.adls.get_file_client(filepath)

        with BytesIO() as buffer:
            file_client.download_file().readinto(buffer)
            buffer.seek(0)
            return json.load(buffer)

    def upload_json_to_adls_json(self, filepath: str, data):
        file_client = self.adls.get_file_client(filepath)
        json_data = json.dumps(data).encode("utf-8")

        with BytesIO(json_data) as buffer:
            buffer.seek(0)
            file_client.upload_data(buffer.getvalue(), overwrite=True)

    def list_paths(self, path: str, recursive=True):
        paths = self.adls.get_paths(path=path, recursive=recursive)
        return list(paths)

    def get_file_metadata(self, filepath: str):
        file_client = self.adls.get_file_client(filepath)
        properties = file_client.get_file_properties()
        return properties


def _get_filepath(source_path: str, dataset_type: str, step: str):
    filename = source_path.split("/")[-1]
    filename = (
        filename.replace(".csv", ".json")
        if step == "data_quality_results"
        else filename
    )

    step_destination_folder_map = {
        "raw": f"{constants.raw_folder}/{dataset_type}",
        "bronze": f"bronze/{dataset_type}",
        "data_quality_results": "logs-gx",
        "dq_split_rows": "bronze/split-rows",
        "dq_passed_rows": f"staging/pending-review/{dataset_type}",
        "dq_failed_rows": "archive/gx-tests-failed",
        "manual_review_passed_rows": (
            f"{constants.staging_approved_folder}/{dataset_type}"
        ),
        "manual_review_failed_rows": (
            f"{constants.archive_manual_review_rejected_folder}"
        ),
        "silver": f"silver/{dataset_type}",
        "gold": "gold",
    }

    destination_folder = step_destination_folder_map[step]

    if not destination_folder:
        raise ValueError(f"Unknown filepath: {source_path}")

    destination_filepath = f"{destination_folder}/{filename}"

    return destination_filepath


def get_output_filepath(context: OpExecutionContext):
    dataset_type = context.get_step_execution_context().op_config["dataset_type"]
    source_path = context.get_step_execution_context().op_config["filepath"]
    step = context.asset_key.to_user_string()

    destination_filepath = _get_filepath(source_path, dataset_type, step)

    return destination_filepath


def get_input_filepath(context: OpExecutionContext) -> str:
    dataset_type = context.get_step_execution_context().op_config["dataset_type"]
    source_path = context.get_step_execution_context().op_config["filepath"]
    filename = source_path.split("/")[-1]
    step = context.asset_key.to_user_string()

    step_origin_folder_map = {
        "bronze": "raw",
        "data_quality_results": "bronze",
        "dq_split_rows": "bronze",
        "dq_passed_rows": "bronze",
        "dq_failed_rows": "bronze",
        "manual_review_passed_rows": "bronze",
        "manual_review_failed_rows": "bronze",
        "silver": "manuaL_review_passed",
        "gold": "silver",
    }

    upstream_step = step_origin_folder_map[step]
    upstream_path = f"{upstream_step}/{dataset_type}/{filename}"

    return upstream_path
