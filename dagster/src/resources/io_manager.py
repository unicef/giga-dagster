import pandas as pd

from dagster import InputContext, IOManager, OutputContext
from src.resources._utils import get_destination_filepath
from src.resources.adls_file_client import ADLSFileClient
from src.settings import AZURE_BLOB_CONNECTION_URI


class StagingADLSIOManager(IOManager):
    def __init__(self):
        self.adls_client = ADLSFileClient()

    def handle_output(self, context: OutputContext, output: pd.DataFrame):
        if output.empty:
            context.log.warning("Output DataFrame is empty. Skipping write operation.")
            return

        filepath = self._get_filepath(context)
        self.adls_client.upload_to_adls(filepath, output)

        if context.step_key == "gold":
            self._create_delta_table(context, output)

        context.log.info(
            f"Uploaded {filepath.split('/')[-1]} to"
            f" {('/').join(filepath.split('/')[:-1])} in ADLS."
        )

    def load_input(self, context: InputContext):
        filepath = self._get_filepath(context.upstream_output)

        context.log.info(
            f"Downloaded {filepath.split('/')[-1]} from"
            f" {('/').join(filepath.split('/')[:-1])} in ADLS."
        )

        return self.adls_client.download_from_adls(filepath)

    def _create_delta_table(self, context: OutputContext, output: pd.DataFrame):
        filename = self._get_filepath(context).split("/")[:-1]
        spark = context.resources.spark
        spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
        spark.sql(
            f"""
                CREATE TABLE IF NOT EXISTS gold.delta_test (
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
                LOCATION '{AZURE_BLOB_CONNECTION_URI}/gold/delta-tables'
            """
        )
        df = output.to_spark(None)
        df.write.format("delta").mode("overwrite").saveAsTable(f"gold.{filename}")

    def _load_test_table(self, context: InputContext):
        filename = self._get_filepath(context.upstream_output).split("/")[:-1]
        spark = context.resources.spark
        df = spark.table(f"gold.{filename}")
        df.show()
        return df

    def _get_filepath(self, context):
        filepath = context.step_context.op_config["filepath"]
        parent_folder = context.step_context.op_config["dataset_type"]
        step = context.step_key

        destination_filepath = get_destination_filepath(filepath, parent_folder, step)

        context.log.info(f"Moving from {filepath} to {destination_filepath}")

        return destination_filepath


# latency connectivity
# admin1
# admin2
# admin3
# admin4


# old columns
# giga_id_school STRING,
# school_id LONG,
# name STRING,
# lat DOUBLE,
# long DOUBLE,
# education_level STRING,
# education_level_regional STRING,
# school_type STRING,
# connectivity STRING,
# coverage_availability STRING
# giga_id_school
