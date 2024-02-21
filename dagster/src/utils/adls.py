import json
from io import BytesIO

import pandas as pd
from delta.tables import DeltaTable
from pyspark import sql
from pyspark.sql import SparkSession

from azure.storage.filedatalake import DataLakeServiceClient
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

            if filepath.endswith(".csv"):
                return pd.read_csv(buffer)
            elif filepath.endswith(".xls") or filepath.endswith(".xlsx"):
                return pd.read_excel(buffer)

    def download_csv_as_spark_dataframe(
        self, filepath: str, spark: SparkSession
    ) -> sql.DataFrame:
        adls_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{filepath}"
        return spark.read.csv(adls_path, header=True, escape='"', multiLine=True)

    def upload_pandas_dataframe_as_file(self, data: pd.DataFrame, filepath: str):
        if len(splits := filepath.split(".")) < 2:
            raise RuntimeError(f"Cannot infer format of file {filepath}")

        file_client = _adls.get_file_client(filepath)
        match splits[-1]:
            case "csv" | "xls" | "xlsx":
                bytes_data = data.to_csv(mode="w+", index=False).encode("utf-8-sig")
            case "json":
                bytes_data = data.to_json().encode()
            case _:
                raise OSError(f"Unsupported format for file {filepath}")

        with BytesIO(bytes_data) as buffer:
            buffer.seek(0)
            file_client.upload_data(buffer.getvalue(), overwrite=True)

    def download_delta_table_as_delta_table(self, table_path: str, spark: SparkSession):
        return DeltaTable.forPath(spark, f"{table_path}")

    def download_delta_table_as_spark_dataframe(
        self, table_path: str, spark: SparkSession
    ):
        df = spark.read.format("delta").load(table_path)
        df.show()
        return df

    def upload_spark_dataframe_as_delta_table(
        self,
        data: sql.DataFrame,
        table_path: str,
        dataset_type: str,
        spark: SparkSession,
    ):
        create_schema_sql = load_sql_template(
            "create_schema",
            schema_name=dataset_type,
        )

        table_name = table_path.split("/")[-1]
        print(f"tablename: {table_name}, table path: {table_path}")

        create_table_sql = load_sql_template(
            f"create_{dataset_type}_table",
            schema_name=dataset_type,
            table_name=table_name,
            location=table_path,
        )
        spark.sql(create_schema_sql)
        spark.sql(create_table_sql)
        data.write.format("delta").mode("overwrite").saveAsTable(
            f"{dataset_type}.{table_name}"
        )

    def download_json(self, filepath: str):
        file_client = _adls.get_file_client(filepath)

        with BytesIO() as buffer:
            file_client.download_file().readinto(buffer)
            buffer.seek(0)
            return json.load(buffer)

    def upload_json(self, data, filepath: str):
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

    def rename_file(self, old_filepath: str, new_filepath: str):
        file_client = _adls.get_file_client(file_path=old_filepath)
        new_path = file_client.file_system_name + "/" + new_filepath
        renamed_file_client = file_client.rename_file(new_name=new_path)
        print(f"File {old_filepath} renamed to {new_path}")
        return renamed_file_client


def get_filepath(source_path: str, dataset_type: str, step: str):
    filename = (
        source_path.split("/")[-1].replace(".csv", ".json")
        if step == "data_quality_results"
        else source_path.split("/")[-1]
    )

    destination_folder = constants.step_destination_folder_map(dataset_type)[step]

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

    upstream_step = constants.step_origin_folder_map[step]
    upstream_path = f"{upstream_step}/{dataset_type}/{filename}"

    return upstream_path
