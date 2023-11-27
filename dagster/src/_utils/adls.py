import json
from io import BytesIO

import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient

from dagster import OpExecutionContext
from src.settings import settings


class ADLSFileClient:
    def __init__(self):
        self.client = DataLakeServiceClient(
            account_url=f"https://{settings.AZURE_BLOB_SAS_HOST}",
            credential=settings.AZURE_SAS_TOKEN,
        )
        self.adls = self.client.get_file_system_client(
            file_system=settings.AZURE_BLOB_CONTAINER_NAME
        )

    def download_adls_csv_to_pandas(self, filepath: str):
        file_client = self.adls.get_file_client(filepath)

        with BytesIO() as buffer:
            file_client.download_file().readinto(buffer)
            buffer.seek(0)
            return pd.read_csv(buffer)

    def upload_pandas_to_adls_csv(self, filepath: str, data: pd.DataFrame):
        file_client = self.adls.get_file_client(filepath)

        with BytesIO() as buffer:
            data.to_csv(buffer, index=False)
            buffer.seek(0)
            file_client.upload_data(buffer.getvalue(), overwrite=True)

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


def _get_filepath(source_path: str, dataset_type: str, step: str):
    filename = source_path.split("/")[-1]
    filename = (
        filename.replace(".csv", ".json")
        if step == "data_quality_results"
        else filename
    )

    step_destination_folder_map = {
        "raw": f"adls-testing-raw/{dataset_type}",
        "bronze": f"bronze/{dataset_type}",
        "data_quality_results": "logs-gx",
        "dq_split_rows": "bronze/split-rows",
        "dq_passed_rows": f"staging/pending-review/{dataset_type}",
        "dq_failed_rows": "archive/gx-tests-failed",
        "manual_review_passed_rows": f"staging/approved/{dataset_type}",
        "manual_review_failed_rows": "archive/manual-review-rejected",
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
