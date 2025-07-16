from pathlib import Path
from typing import Union

import pandas as pd
from delta import DeltaTable
from pyspark import sql
from pyspark.sql.types import StructType

from azure.core.exceptions import ResourceNotFoundError
from dagster import InputContext, OutputContext
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.delta import execute_query_with_error_handler
from src.utils.op_config import FileConfig
from src.utils.schema import construct_full_table_name

from .adls_delta import ADLSDeltaIOManager

adls_client = ADLSFileClient()


class ADLSDeltaIntermediaryIOManager(ADLSDeltaIOManager):
    """
    A unified ADLS Delta IO Manager that can handle both Pandas and Spark DataFrames.

    This manager:
    1. Stores data primarily in Delta tables for queryability and robustness
    2. Creates single-file outputs (CSV/JSON/Parquet) at computed filepaths for compatibility
    3. Supports both Pandas and Spark DataFrame inputs/outputs with automatic conversion
    4. Provides drop-in replacement functionality for ADLSPandasIOManager
    """

    def handle_output(
        self, context: OutputContext, output: Union[sql.DataFrame, pd.DataFrame, None]
    ):
        """
        Handles the output by:
        1. Converting input to Spark DataFrame if needed
        2. Creating Delta table and writing data
        3. Creating compatible output using intelligent I/O routing
        """
        context.log.info("Handling IntermediaryDeltaIOManager output...")
        if output is None:
            context.log.info("Output is None, skipping execution.")
            return

        config = FileConfig(**context.step_context.op_config)
        spark = self._get_spark_session()

        # Convert to Spark DataFrame if needed
        if isinstance(output, pd.DataFrame):
            context.log.info("Converting Pandas DataFrame to Spark DataFrame")
            spark_df = spark.createDataFrame(output)
        else:
            spark_df = output

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

        # 1. Create and save to Delta Table
        self._create_schema_if_not_exists(schema_tier_name)
        # Use the overridden _create_table_if_not_exists specific to this class
        self._create_table_if_not_exists(
            context, spark_df, schema_tier_name, table_name
        )

        # Always overwrite for intermediary tables
        self._overwrite_data(
            spark_df,
            config.metastore_schema,  # Keep original schema for metadata purposes if needed
            full_table_name,  # Use the enforced full table name for the operation
            context,
        )

        context.log.info(
            f"Overwritten {table_name} at {settings.SPARK_WAREHOUSE_DIR}/{schema_tier_name}.db in ADLS.",
        )

        # 2. Create compatible output using intelligent I/O routing
        self._save_compatible_output(context, output)

    def _save_compatible_output(
        self,
        context: OutputContext,
        output: Union[sql.DataFrame, pd.DataFrame],
    ):
        """
        Save the data using intelligent I/O routing based on data type and format.

        This method:
        - Uses single-file I/O for Pandas DataFrames
        - Uses distributed I/O for Spark DataFrames with supported formats
        - Falls back to single-file I/O for unsupported Spark formats (like Excel)

        This ensures drop-in replacement functionality while optimizing performance.
        """
        # Get the filepath that would be used by ADLSPandasIOManager
        filepath = self._get_filepath(context)
        file_ext = Path(filepath).suffix.lower()
        context.log.info(
            f"Saving compatible output to: {filepath} (format: {file_ext})"
        )

        try:
            if isinstance(output, pd.DataFrame):
                # Pandas DataFrame: Always use single-file I/O
                context.log.info("Using single-file I/O for Pandas DataFrame")
                adls_client.upload_pandas_dataframe_as_file(
                    context=context,
                    data=output,
                    filepath=str(filepath),
                )
                context.log.info(f"Successfully saved Pandas DataFrame to {filepath}")

            elif isinstance(output, sql.DataFrame):
                # Spark DataFrame: Use intelligent routing based on format
                distributed_formats = {".parquet", ".csv", ".json"}

                if file_ext in distributed_formats:
                    # Use distributed I/O for supported formats
                    context.log.info(
                        f"Using distributed I/O for Spark DataFrame (format: {file_ext})"
                    )

                    # Transform logical filepath to directory path
                    # e.g., "my_data.parquet" -> "my_data/"
                    directory_path = str(Path(filepath).with_suffix(""))
                    file_format = file_ext.lstrip(".")  # Remove the dot

                    adls_client.upload_spark_dataframe_as_files(
                        spark_df=output,
                        directory_path=directory_path,
                        file_format=file_format,
                        mode="overwrite",
                    )
                    context.log.info(
                        f"Successfully saved Spark DataFrame to distributed directory: {directory_path}"
                    )

                else:
                    # Unsupported format for distributed I/O (e.g., Excel)
                    # Fall back to single-file I/O via Pandas conversion
                    context.log.warning(
                        f"Format {file_ext} not supported for distributed I/O. "
                        f"Converting Spark DataFrame to Pandas for single-file upload. "
                        f"This may impact performance for large datasets."
                    )

                    # Check dataset size and warn if large
                    row_count = output.count()
                    if row_count > 100000:
                        context.log.warning(
                            f"Converting large Spark DataFrame ({row_count} rows) to Pandas. "
                            "Consider using a supported distributed format (parquet, csv, json) for better performance."
                        )

                    pandas_converted = output.toPandas()
                    adls_client.upload_pandas_dataframe_as_file(
                        context=context,
                        data=pandas_converted,
                        filepath=str(filepath),
                    )
                    context.log.info(
                        f"Successfully saved Spark DataFrame as single file to {filepath}"
                    )

            else:
                raise ValueError(f"Unsupported output type: {type(output)}")

        except Exception as e:
            context.log.error(f"Failed to save compatible output: {e}")
            # Don't raise the error to prevent the main Delta Table functionality from failing
            context.log.info(
                "Delta table write succeeded, but compatible output failed. Data is still available in Delta table."
            )

    def load_input(self, context: InputContext) -> sql.DataFrame:
        """
        Loads input data from the corresponding Delta Table.
        """
        context.log.info("Handling IntermediaryDeltaIOManager input...")

        # Use asset key's last element as the base table name
        asset_name = context.asset_key.path[-1]
        context.log.info(f"Using asset name for base table: {asset_name}")
        spark = self._get_spark_session()

        # Config retrieval might need adjustment depending on how upstream config is passed
        # Attempt to get config from upstream output context first
        upstream_config = {}
        if context.upstream_output:
            upstream_config = context.upstream_output.step_context.op_config
        else:
            # Fallback to current step context if no upstream (e.g., source asset)
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

        # Load from Delta table
        try:
            dt = DeltaTable.forName(spark, full_table_name)
            context.log.info(
                f"Loaded {table_name} from Delta table at {settings.SPARK_WAREHOUSE_DIR}/{schema_tier_name}.db in ADLS.",
            )
            return dt.toDF()
        except Exception as e:
            context.log.error(
                f"Could not load from Delta table {full_table_name}: {e}"
            )
            raise

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
