import pandas as pd

from dagster import InputContext, IOManager, OutputContext
from src._utils import get_spark_session
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
            self._create_delta_table(context, output, filepath)

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
        spark = get_spark_session()
        filename = self._get_filepath(context).split("/")[:-1]
        spark.sql(
            f"""
            CREATE OR REPLACE TABLE gold.delta_table (
                id LONG
                giga_id_school VARCHAR(100)
                school_id VARCHAR(100)
                name VARCHAR(100)
                education_level VARCHAR(100)
                lat DECIMAL(18, 18)
                lon DECIMAL(18, 18)
                connectivity VARCHAR(20)
                type_connectivity VARCHAR(100)
                connectivity_speed INT(20)
                num_students INT(20)
                num_computers INT(20)
                electricity VARCHAR(20)
                computer_availability VARCHAR(20)
                education_level_regional VARCHAR(20)
                school_type VARCHAR(20)
                coverage_availability VARCHAR(20)
                coverage_type VARCHAR(20)
                latency_connectivity VARCHAR(100)
                admin1 VARCHAR(100)
                admin2 VARCHAR(100)
                admin3 VARCHAR(100)
                admin4 VARCHAR(100)
                school_region VARCHAR(100)
                num_teachers INT(20)
                num_classroom INT(20)
                computer_lab VARCHAR(20)
                water VARCHAR(20)
                address VARCHAR(100)
                fiber_node_distance DECIMAL(18, 18)
                microwave_node_distance DECIMAL(18, 18)
                nearest_school_distance DECIMAL(18, 18)
                schools_within_1km INT(20)
                schools_within_2km INT(20)
                schools_within_3km INT(20)
                schools_within_10km INT(20)
                nearest_LTE_id INT(20)
                nearest_LTE_distance DECIMAL(18, 18)
                nearest_UMTS_id INT(20)
                nearest_UMTS_distance DECIMAL(18, 18)
                nearest_GSM_id INT(20)
                nearest_GSM_distance DECIMAL(18, 18)
                pop_within_1km INT(20)
                pop_within_2km INT(20)
                pop_within_3km INT(20)
                pop_within_10km INT(20)
            )
            USING DELTA
            LOCATION '{AZURE_BLOB_CONNECTION_URI}/gold/delta-test'
            """
        )
        data = output.to_spark(None)
        data.write.format("delta").mode("overwrite").saveAsTable(f"gold.{filename}")
        return

    def _load_delta_table(self, context: InputContext) -> pd.DataFrame:
        spark = get_spark_session()
        filename = self._get_filepath(context.upstream_output).split("/")[:-1]
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


# no type
# education_level_regional VARCHAR(20)
# school_type VARCHAR(20)
#  admin1 VARCHAR(100)
#                 admin2 VARCHAR(100)
#                 admin3 VARCHAR(100)
#                 # admin4 VARCHAR(100)
