import pandas as pd

from dagster import InputContext, IOManager, OutputContext
from src._utils.adls import ADLSFileClient, _get_filepath
from src.settings import settings


class StagingADLSIOManager(IOManager):
    def __init__(self):
        self.adls_client = ADLSFileClient()

    def handle_output(self, context: OutputContext, output: pd.DataFrame):
        filepath = self._get_filepath(context)
        if context.step_key != "data_quality_results":
            if output.empty:
                context.log.warning(
                    "Output DataFrame is empty. Skipping write operation."
                )
                return
            self.adls_client.upload_pandas_to_adls_csv(filepath, output)
            context.log.info("uploaded csv")
        else:
            self.adls_client.upload_json_to_adls_json(filepath, output)
            context.log.info("uploaded json")

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

        if (
            context.upstream_output.step_key == "data_quality_results"
            and context.asset_key.to_user_string() == "data_quality_results"
        ):
            file = self.adls_client.download_adls_json_to_json(filepath)
            context.log.info(f"downloaded json: {file}")
            return file
        else:
            file = self.adls_client.download_adls_csv_to_pandas(filepath)
            context.log.info(f"downloaded csv: {file}")
            return file

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
                LOCATION f'{settings.AZURE_BLOB_CONNECTION_URI}/gold/delta-tables'
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

        destination_filepath = _get_filepath(filepath, parent_folder, step)

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
