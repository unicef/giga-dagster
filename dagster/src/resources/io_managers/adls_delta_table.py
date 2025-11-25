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


class ADLSDeltaTableIOManager(ADLSDeltaIOManager):
    """
    An ADLS Delta Table IO Manager designed for intermediary outputs where schema
    validation against a predefined schema is not required, and data is
    stored in unique tables based on custom schema configuration or fallback naming.
    """

    def _get_schema_and_table_name(
        self, config: FileConfig, asset_name: str
    ) -> tuple[str, str]:
        """
        Generate schema and table names using custom configuration or fallback naming.

        Args:
            config: FileConfig containing custom configuration
            asset_name: Name of the asset as fallback

        Returns:
            Tuple of (schema_name, table_name)
        """
        file_upload_id = config.filename_components.id
        country_code = config.country_code.lower()

        # Check if custom_schema_name is provided in configuration
        if hasattr(config, "custom_schema_name") and config.custom_schema_name:
            schema_name = config.custom_schema_name
            table_name = f"{file_upload_id}_{country_code}"
        else:
            # Fallback: use asset name as schema name
            schema_name = asset_name
            table_name = f"{file_upload_id}_{country_code}"

        return schema_name, table_name

    def handle_output(self, context: OutputContext, output: sql.DataFrame | None):
        """
        Handles the output by creating the schema and table if they don't exist
        (using the DataFrame's schema) and then writing the data to a deterministic
        table name. Schema evolution is handled by Delta Lake's overwriteSchema option.
        """
        context.log.info("Handling ADLSDeltaIntermediaryIOManager output...")
        if output is None:
            context.log.info("Output is None, skipping execution.")
            return

        config = FileConfig(**context.step_context.op_config)

        # Generate schema and table names
        asset_name = context.asset_key.path[-1]
        schema_name, table_name = self._get_schema_and_table_name(config, asset_name)

        context.log.info(f"Using schema: {schema_name}")
        context.log.info(f"Using table name: {table_name}")

        full_table_name = f"{schema_name}.{table_name}"
        context.log.info(f"Full table name: {full_table_name}")

        self._create_schema_if_not_exists(schema_name)
        # Use the overridden _create_table_if_not_exists specific to this class
        self._create_table_if_not_exists(context, output, schema_name, table_name)

        # Write data to the deterministic table (overwrite since file_upload_id ensures uniqueness per run)
        self._overwrite_data(
            output,
            config.metastore_schema,  # Keep original schema for metadata purposes if needed
            full_table_name,  # Use the full table name for the operation
            context,
        )

        context.log.info(
            f"Written data to {table_name} at {settings.SPARK_WAREHOUSE_DIR}/{schema_name}.db in ADLS.",
        )

    def load_input(self, context: InputContext) -> sql.DataFrame:
        """
        Loads input data from an intermediary Delta table by reconstructing the
        deterministic table name from upstream asset information.
        """
        context.log.info("Handling ADLSDeltaIntermediaryIOManager input...")

        if not context.upstream_output:
            raise ValueError(
                "No upstream output found. Cannot determine table name for intermediary IO manager."
            )

        if (
            not hasattr(context.upstream_output, "step_context")
            or not context.upstream_output.step_context
        ):
            raise ValueError(
                "Upstream step context not available. Cannot reconstruct table name for intermediary IO manager."
            )

        upstream_asset_name = context.upstream_output.asset_key.path[-1]
        run_config = context.step_context.run_config
        # Use upstream_asset_name to look up the correct upstream config
        upstream_op_config = run_config["ops"].get(upstream_asset_name)

        if upstream_op_config is None:
            raise ValueError(
                f"Upstream operation config not found for asset "
                f"'{upstream_asset_name}'. Cannot determine table name for "
                "intermediary IO manager."
            )

        upstream_config = FileConfig(**upstream_op_config["config"])

        schema_name, table_name = self._get_schema_and_table_name(
            upstream_config, upstream_asset_name
        )
        full_table_name = f"{schema_name}.{table_name}"

        context.log.info(f"Loading data from {full_table_name}")

        spark = self._get_spark_session()
        dt = DeltaTable.forName(spark, full_table_name)

        return dt.toDF()

    def _create_table_if_not_exists(
        self,
        context: OutputContext,
        data: sql.DataFrame,
        schema_name: str,
        table_name: str,
    ):
        """
        Creates a Delta table if it doesn't exist, using the schema derived
        directly from the input DataFrame. Defaults to no partitioning.
        Inherits Change Data Feed and retention properties from base class logic.
        """
        spark = self._get_spark_session()
        full_table_name = construct_full_table_name(schema_name, table_name)

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

        execute_query_with_error_handler(spark, query, schema_name, table_name, context)
