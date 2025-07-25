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


class ADLSDeltaIntermediaryIOManager(ADLSDeltaIOManager):
    """
    An ADLS Delta IO Manager designed for intermediary outputs where schema
    validation against a predefined schema is not required, and data is
    stored in unique tables based on asset name, file upload ID, and country code.
    """

    def _generate_table_name(
        self, asset_name: str, file_upload_id: str, country_code: str
    ) -> str:
        """
        Generate a deterministic table name using asset name, file upload ID, and country code.

        Args:
            asset_name: Name of the asset
            file_upload_id: Unique file upload identifier
            country_code: Country code (will be lowercased for consistency)

        Returns:
            Formatted table name: {asset_name}_{file_upload_id}_{country_code}
        """
        return f"{asset_name}_{file_upload_id}_{country_code.lower()}"

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

        # Generate deterministic table name
        asset_name = context.asset_key.path[-1]
        country_code = config.country_code.lower()  # Ensure consistency
        file_upload_id = config.filename_components.id

        table_name = self._generate_table_name(asset_name, file_upload_id, country_code)
        context.log.info(f"Using deterministic table name: {table_name}")

        # Enforce an 'int_' prefix for the schema name, ignoring the configured tier
        schema_tier_name = f"int_{config.metastore_schema}"
        context.log.info(f"Enforcing intermediary schema: {schema_tier_name}")

        full_table_name = f"{schema_tier_name}.{table_name}"
        context.log.info(f"Full table name: {full_table_name}")

        self._create_schema_if_not_exists(schema_tier_name)
        # Use the overridden _create_table_if_not_exists specific to this class
        self._create_table_if_not_exists(context, output, schema_tier_name, table_name)

        # Write data to the deterministic table (overwrite since file_upload_id ensures uniqueness per run)
        self._overwrite_data(
            output,
            config.metastore_schema,  # Keep original schema for metadata purposes if needed
            full_table_name,  # Use the enforced full table name for the operation
            context,
        )

        context.log.info(
            f"Written data to {table_name} at {settings.SPARK_WAREHOUSE_DIR}/{schema_tier_name}.db in ADLS.",
        )

    def load_input(self, context: InputContext) -> sql.DataFrame:
        """
        Loads input data from an intermediary Delta table by reconstructing the
        deterministic table name from upstream asset information.
        """
        context.log.info("Handling ADLSDeltaIntermediaryIOManager input...")

        # Reconstruct table name from upstream asset information
        if not context.upstream_output:
            raise ValueError(
                "No upstream output found. Cannot determine table name for intermediary IO manager."
            )

        # Get upstream asset information
        upstream_asset_name = context.upstream_output.asset_key.path[-1]

        # Get upstream config to extract file_upload_id and country_code
        if (
            not hasattr(context.upstream_output, "step_context")
            or not context.upstream_output.step_context
        ):
            raise ValueError(
                "Upstream step context not available. Cannot reconstruct table name for intermediary IO manager."
            )

        upstream_config = FileConfig(**context.upstream_output.step_context.op_config)
        file_upload_id = upstream_config.filename_components.id
        country_code = upstream_config.country_code.lower()

        # Generate the same table name format as handle_output
        table_name = self._generate_table_name(
            upstream_asset_name, file_upload_id, country_code
        )

        # Enforce the same 'int_' prefix for schema name
        schema_tier_name = f"int_{upstream_config.metastore_schema}"
        full_table_name = f"{schema_tier_name}.{table_name}"

        context.log.info(
            f"Reconstructed table name from upstream context: {full_table_name}"
        )

        spark = self._get_spark_session()
        dt = DeltaTable.forName(spark, full_table_name)

        context.log.info(f"Successfully loaded data from {full_table_name}")

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
