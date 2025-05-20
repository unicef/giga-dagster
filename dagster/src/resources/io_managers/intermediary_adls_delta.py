from datetime import datetime

from delta import DeltaTable
from pyspark import sql

from dagster import InputContext, OutputContext
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.delta import execute_query_with_error_handler
from src.utils.op_config import FileConfig
from src.utils.schema import construct_full_table_name

from .adls_delta import ADLSDeltaIOManager

adls_client = ADLSFileClient()


class IntermediaryADLSDeltaIOManager(ADLSDeltaIOManager):
    """
    An ADLS Delta IO Manager designed for intermediary outputs where schema
    validation against a predefined schema is not required, and data is
    typically overwritten.

    Additionally, this IO manager saves a copy of the data as CSV files
    in a timestamped folder to allow for historical tracking of intermediate results.
    """

    def handle_output(self, context: OutputContext, output: sql.DataFrame | None):
        """
        Handles the output by:
        1. Creating the schema and table if they don't exist and writing to Delta Table
        2. Additionally saving the data as CSV files in a timestamped folder
        """
        context.log.info("Handling IntermediaryDeltaIOManager output...")
        if output is None:
            context.log.info("Output is None, skipping execution.")
            return

        config = FileConfig(**context.step_context.op_config)
        # Use asset key's last element as the base table name
        asset_name = context.asset_key.path[-1]
        country_code = config.country_code.lower()  # Ensure consistency
        table_name = f"{asset_name}_{country_code}"
        context.log.info(f"Using asset and country code for table: {table_name}")
        # Enforce an 'int_' prefix for the schema name, ignoring the configured tier
        schema_tier_name = f"int_{config.metastore_schema}"
        context.log.info(f"Enforcing intermediary schema: {schema_tier_name}")

        full_table_name = f"{schema_tier_name}.{table_name}"
        context.log.info(f"Full table name: {full_table_name}")

        # Skip truncation logic present in the base class handle_output
        # if self._handle_truncate(context, output, config, full_table_name):
        #     return

        # 1. Create and save to Delta Table as before
        self._create_schema_if_not_exists(schema_tier_name)
        # Use the overridden _create_table_if_not_exists specific to this class
        self._create_table_if_not_exists(context, output, schema_tier_name, table_name)

        # Always overwrite for intermediary tables
        self._overwrite_data(
            output,
            config.metastore_schema,  # Keep original schema for metadata purposes if needed
            full_table_name,  # Use the enforced full table name for the operation
            context,
        )

        context.log.info(
            f"Overwritten {table_name} at {settings.SPARK_WAREHOUSE_DIR}/{schema_tier_name}.db in ADLS.",
        )

        # 2. Additionally save as CSV files with timestamp
        self._save_data_as_files(context, output, schema_tier_name, table_name)

    def _save_data_as_files(
        self,
        context: OutputContext,
        data: sql.DataFrame,
        schema_tier_name: str,
        table_name: str,
    ):
        """
        Save the data as CSV files in a timestamped folder structure.

        The folder structure will be:
        /intermediary/{schema_tier_name}/{table_name}/{timestamp}/
        """
        # Generate timestamp for this execution
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Define the folder path with schema, table, and timestamp
        folder_path = f"intermediary/{schema_tier_name}/{table_name}/{timestamp}"

        context.log.info(f"Saving CSV files to folder: {folder_path}")

        try:
            # Use the new method to upload the spark dataframe as CSV files
            adls_client.upload_spark_dataframe_as_files(
                context=context, data=data, folder_path=folder_path, file_format="csv"
            )

            context.log.info(f"Successfully saved CSV files to {folder_path}")
        except Exception as e:
            context.log.error(f"Failed to save CSV files: {e}")
            # Don't raise the error to prevent the main Delta Table functionality from failing

    def load_input(self, context: InputContext) -> sql.DataFrame:
        """
        Loads input data from an intermediary Delta table, ensuring the
        'int_' prefix is used for the schema name.
        """
        context.log.info("Handling IntermediaryDeltaIOManager input...")

        # Use asset key's last element as the base table name
        asset_name = context.asset_key.path[-1]
        context.log.info(f"Using asset name for base table: {asset_name}")
        spark = self._get_spark_session()
        # Config retrieval might need adjustment depending on how upstream config is passed
        # Assuming config is available similarly to how it's accessed in the base class
        # Attempt to get config from upstream output context first
        upstream_config = {}
        if context.upstream_output:
            upstream_config = context.upstream_output.step_context.op_config
        else:
            # Fallback to current step context if no upstream (e.g., source asset)
            # This might need refinement based on actual usage patterns
            upstream_config = context.step_context.op_config
            context.log.warning(
                "No upstream output found, using current step config for schema name. "
                "This might be incorrect if the IO manager is used on a source asset."
            )

        # Ensure upstream_config is not None before creating FileConfig
        if upstream_config is None:
            raise ValueError(
                "Could not determine upstream configuration for IO Manager."
            )

        config = FileConfig(**upstream_config)
        country_code = config.country_code.lower()  # Ensure consistency
        # Construct the table name including the country code
        table_name = f"{asset_name}_{country_code}"
        context.log.info(f"Derived table name for input: {table_name}")
        # Enforce the 'int_' prefix for the schema name
        schema_tier_name = f"int_{config.metastore_schema}"
        context.log.info(f"Enforcing intermediary schema for input: {schema_tier_name}")

        full_table_name = construct_full_table_name(schema_tier_name, table_name)
        context.log.info(f"Full table name for input: {full_table_name}")

        dt = DeltaTable.forName(spark, full_table_name)

        context.log.info(
            f"Loaded {table_name} from {settings.SPARK_WAREHOUSE_DIR}/{schema_tier_name}.db in ADLS.",
        )

        return dt.toDF()

    def _create_table_if_not_exists(
        self,
        context: OutputContext,
        data: sql.DataFrame,
        schema_name_for_tier: str,
        table_name: str,
    ):
        """
        Creates a Delta table if it doesn't exist, using the schema derived
        directly from the input DataFrame. Defaults to no partitioning.
        Inherits Change Data Feed and retention properties from base class logic.
        """
        spark = self._get_spark_session()
        full_table_name = construct_full_table_name(schema_name_for_tier, table_name)

        # Use the schema directly from the input data
        columns = data.schema.fields
        # Default to no partitioning for intermediary tables, unless specified differently
        partition_columns = []  # TODO: Potentially make configurable if needed

        query = (
            DeltaTable.createIfNotExists(spark)
            .tableName(full_table_name)
            .addColumns(columns)
        )

        if len(partition_columns) > 0:
            query = query.partitionedBy(*partition_columns)

        # Reuse property settings from the base class method if appropriate
        # For simplicity, copying relevant properties here.
        # Consider refactoring base class if more properties need sharing.
        query = query.property("delta.enableChangeDataFeed", "true")
        # Assuming default retention is acceptable for intermediary, or adjust as needed
        # query = query.property("delta.logRetentionDuration", constants.default_retention_period)

        execute_query_with_error_handler(
            spark, query, schema_name_for_tier, table_name, context
        )
