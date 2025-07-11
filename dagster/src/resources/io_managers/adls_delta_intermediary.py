from datetime import datetime
from pathlib import Path
from typing import Union

import pandas as pd
from delta import DeltaTable
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from azure.core.exceptions import ResourceNotFoundError
from dagster import InputContext, OutputContext
from src.settings import settings
from src.constants import DataTier
from src.utils.adls import ADLSFileClient
from src.utils.delta import check_table_exists, execute_query_with_error_handler
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
        3. Creating single-file output at computed filepath for compatibility
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

        # 2. Create single-file output at computed filepath for compatibility
        self._save_single_file_output(context, spark_df)

    def _save_single_file_output(
        self,
        context: OutputContext,
        spark_df: sql.DataFrame,
    ):
        """
        Save the data as a single file at the computed filepath for compatibility with ADLSPandasIOManager.

        This ensures drop-in replacement functionality by creating files at the same paths
        that ADLSPandasIOManager would use.
        """
        # Get the filepath that would be used by ADLSPandasIOManager
        filepath = self._get_filepath(context)
        context.log.info(f"Saving single file output to: {filepath}")

        try:
            # Convert Spark DataFrame to Pandas for single-file upload
            # For large datasets, this might be memory-intensive, but ensures compatibility
            if spark_df.count() > 100000:  # Warn for large datasets
                context.log.warning(
                    f"Converting large Spark DataFrame ({spark_df.count()} rows) to Pandas for single-file upload. "
                    "Consider using Spark output type for better performance."
                )

            pandas_converted = spark_df.toPandas()
            adls_client.upload_pandas_dataframe_as_file(
                context=context,
                data=pandas_converted,
                filepath=str(filepath),
            )
            context.log.info(f"Successfully saved single file output to {filepath}")

        except Exception as e:
            context.log.error(f"Failed to save single file output: {e}")
            # Don't raise the error to prevent the main Delta Table functionality from failing

    def load_input(self, context: InputContext) -> sql.DataFrame:
        """
        Loads input data with fallback strategy:
        1. First try to load from Delta table
        2. If Delta table doesn't exist, try to load from file
        3. Return Spark DataFrame in all cases
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

        # Try to load from Delta table first
        try:
            # Check if table exists by trying to access it directly
            dt = DeltaTable.forName(spark, full_table_name)
            context.log.info(
                f"Loaded {table_name} from Delta table at {settings.SPARK_WAREHOUSE_DIR}/{schema_tier_name}.db in ADLS.",
            )
            return dt.toDF()
        except Exception as e:
            context.log.warning(f"Could not load from Delta table: {e}. Trying file.")

        # Fallback to file-based loading
        filepath = self._get_filepath(context)
        try:
            context.log.info(f"Attempting to load from file: {filepath}")

            # Try to load as Spark DataFrame from file
            data = adls_client.download_csv_as_spark_dataframe(str(filepath), spark)
            context.log.info(f"Successfully loaded from file: {filepath}")
            return data

        except ResourceNotFoundError:
            # Handle reference files that might not exist
            if "_reference_" in context.asset_key.to_user_string():
                context.log.warning(
                    f"Reference file {filepath} does not exist, returning empty DataFrame."
                )
                return spark.createDataFrame([], StructType())
            raise
        except Exception as e:
            context.log.error(f"Failed to load from file {filepath}: {e}")
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
