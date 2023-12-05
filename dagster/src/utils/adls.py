import json
from io import BytesIO

import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark import sql
from pyspark.sql import SparkSession

from dagster import ConfigurableResource, OpExecutionContext
from src.constants import constants
from src.settings import settings
from src.utils.sql import load_sql_template

_client = DataLakeServiceClient(
    account_url=f"https://{settings.AZURE_DFS_SAS_HOST}",
    credential=settings.AZURE_SAS_TOKEN,
)
_adls = _client.get_file_system_client(file_system=settings.AZURE_BLOB_CONTAINER_NAME)


class ADLSFileClient(ConfigurableResource):
    def download_csv_as_pandas_dataframe(self, filepath: str) -> pd.DataFrame:
        file_client = _adls.get_file_client(filepath)
        with BytesIO() as buffer:
            file_client.download_file().readinto(buffer)
            buffer.seek(0)
            return pd.read_csv(buffer)

    def download_csv_as_spark_dataframe(
        self, filepath: str, spark: SparkSession
    ) -> sql.DataFrame:
        adls_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{filepath}"
        return spark.read.csv(adls_path, header=True)

    def upload_pandas_dataframe_as_file(self, filepath: str, data: pd.DataFrame):
        if len(splits := filepath.split(".")) < 2:
            raise RuntimeError(f"Cannot infer format of file {filepath}")

        file_client = _adls.get_file_client(filepath)
        match splits[-1]:
            case "csv" | "xls" | "xlsx":
                bytes_data = data.to_csv().encode("utf-8-sig")
            case "json":
                bytes_data = data.to_json().encode()
            case _:
                raise IOError(f"Unsupported format for file {filepath}")

        with BytesIO(bytes_data) as buffer:
            buffer.seek(0)
            file_client.upload_data(buffer.getvalue(), overwrite=True)

    def upload_spark_dataframe_as_csv(self, data: sql.DataFrame, filepath: str):
        data.write.csv(filepath, header=True, mode="overwrite")

    def download_delta_table_as_spark_dataframe(
        self, filepath: str, spark: SparkSession
    ):
        adls_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{filepath}"
        df = spark.read.format("delta").load(adls_path)
        df.show()

    def upload_spark_dataframe_as_delta_table(
        self, data: sql.DataFrame, filepath: str, spark: SparkSession
    ):
        filename = filepath.split("/")[-1]
        country_code = filename.split("_")[0]

        # TODO: Get from context
        schema_name = "school_data"
        create_schema_sql = load_sql_template(
            "create_gold_schema",
            schema_name=schema_name,
        )
        create_table_sql = load_sql_template(
            "create_gold_table",
            schema_name=schema_name,
            table_name=country_code,
            location=(
                f"{settings.AZURE_BLOB_CONNECTION_URI}/gold/delta-tables/{country_code}"
            ),
        )
        spark.sql(create_schema_sql)
        spark.sql(create_table_sql)
        data.write.format("delta").mode("overwrite").saveAsTable(
            f"{schema_name}.{country_code}"
        )

    def download_json(self, filepath: str):
        file_client = _adls.get_file_client(filepath)

        with BytesIO() as buffer:
            file_client.download_file().readinto(buffer)
            buffer.seek(0)
            return json.load(buffer)

    def upload_json(self, filepath: str, data):
        file_client = _adls.get_file_client(filepath)
        json_data = json.dumps(data).encode("utf-8")

        with BytesIO(json_data) as buffer:
            buffer.seek(0)
            file_client.upload_data(buffer.getvalue(), overwrite=True)

    def list_paths(self, path: str, recursive=True):
        paths = _adls.get_paths(path=path, recursive=recursive)
        return list(paths)

    def get_file_metadata(self, filepath: str):
        file_client = _adls.get_file_client(filepath)
        properties = file_client.get_file_properties()
        return properties


def get_filepath(source_path: str, dataset_type: str, step: str):
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

    destination_filepath = get_filepath(source_path, dataset_type, step)

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
        "silver": "manual_review_passed",
        "gold": "silver",
    }

    upstream_step = step_origin_folder_map[step]
    upstream_path = f"{upstream_step}/{dataset_type}/{filename}"

    return upstream_path