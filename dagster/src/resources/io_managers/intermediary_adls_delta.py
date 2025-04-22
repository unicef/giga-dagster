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
    """

    def handle_output(self, context: OutputContext, output: sql.DataFrame | None):
        """
        Handles the output by creating the schema and table if they don't exist
        (using the DataFrame's schema) and then overwriting the table's contents.
        Schema evolution is handled by Delta Lake's overwriteSchema option.
        """
        context.log.info("Handling IntermediaryDeltaIOManager output...")
        if output is None:
            context.log.info("Output is None, skipping execution.")
            return

        config = FileConfig(**context.step_context.op_config)
        table_name, _, _ = self._get_table_path(context)
        # Enforce an 'int_' prefix for the schema name, ignoring the configured tier
        schema_tier_name = f"int_{config.metastore_schema}"
        context.log.info(f"Enforcing intermediary schema: {schema_tier_name}")

        full_table_name = f"{schema_tier_name}.{table_name}"
        context.log.info(f"Full table name: {full_table_name}")

        # Skip truncation logic present in the base class handle_output
        # if self._handle_truncate(context, output, config, full_table_name):
        #     return

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

    def load_input(self, context: InputContext) -> sql.DataFrame:
        """
        Loads input data from an intermediary Delta table, ensuring the
        'int_' prefix is used for the schema name.
        """
        context.log.info("Handling IntermediaryDeltaIOManager input...")

        table_name, _, _ = self._get_table_path(context)
        spark = self._get_spark_session()
        # Config retrieval might need adjustment depending on how upstream config is passed
        # Assuming config is available similarly to how it's accessed in the base class
        config = FileConfig(**context.upstream_output.step_context.op_config)
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
