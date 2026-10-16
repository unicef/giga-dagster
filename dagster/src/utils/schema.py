from models.file_upload import FileUpload
from models.schema_registry import SchemaVersionColumn
from pyspark.sql import Column, SparkSession
from pyspark.sql.functions import when
from pyspark.sql.types import DataType, StringType, StructField
from sqlalchemy import select

from dagster import (
    AssetExecutionContext,
    InputContext,
    OpExecutionContext,
    OutputContext,
)
from src.constants import DataTier, constants
from src.schemas.file_upload import FileUploadConfig
from src.utils.db.primary import get_db_context
from src.utils.op_config import FileConfig
from src.utils.schema_resolver import (
    PostgresSchemaRegistry,
    primary_key_name,
    resolve_schema_columns,
    resolve_schema_columns_for_upload,
)

_UPLOAD_PINNED_TIERS = {DataTier.BRONZE, DataTier.DATA_QUALITY_CHECKS, DataTier.STAGING}


def get_schema_name(
    context: InputContext | OutputContext | OpExecutionContext | AssetExecutionContext,
) -> str:
    if isinstance(context, InputContext | OutputContext):
        return context.step_context.op_config["metastore_schema"]
    return context.op_config["metastore_schema"]


def get_resolved_schema_columns(
    schema_name: str,
    *,
    schema_version: int | None = None,
    core_schema_key: str | None = None,
    core_schema_version: int | None = None,
    resolve_current_core: bool = True,
) -> list[SchemaVersionColumn]:
    with get_db_context() as db:
        return resolve_schema_columns(
            PostgresSchemaRegistry(db),
            schema_name,
            schema_version=schema_version,
            core_schema_key=core_schema_key,
            core_schema_version=core_schema_version,
            resolve_current_core=resolve_current_core,
        )


def get_resolved_schema_columns_for_upload(
    fallback_schema_name: str,
    file_upload: FileUploadConfig,
) -> list[SchemaVersionColumn]:
    with get_db_context() as db:
        return resolve_schema_columns_for_upload(
            PostgresSchemaRegistry(db),
            fallback_schema_name,
            file_upload,
        )


def try_load_file_upload_config(upload_id: str | None) -> FileUploadConfig | None:
    if not upload_id:
        return None
    with get_db_context() as db:
        file_upload = db.scalar(select(FileUpload).where(FileUpload.id == upload_id))
        if file_upload is None:
            return None
        return FileUploadConfig.from_orm(file_upload)


def _mapped_type(data_type: str):
    normalized = "integer" if data_type == "int" else data_type
    mapping = getattr(constants.TYPE_MAPPINGS, normalized, None)
    if mapping is None:
        raise ValueError(f"Unsupported schema data type '{data_type}'")
    return mapping


def _struct_fields(columns: list[SchemaVersionColumn]) -> list[StructField]:
    return [
        StructField(
            column.name,
            _mapped_type(column.data_type).pyspark(),
            column.is_nullable is not False,
        )
        for column in columns
    ]


def _datahub_fields(columns: list[SchemaVersionColumn]) -> list[tuple]:
    return [
        (column.name, _mapped_type(column.data_type).datahub()) for column in columns
    ]


def get_schema_columns(
    spark: SparkSession,
    schema_name: str,
    *,
    schema_version: int | None = None,
    core_schema_key: str | None = None,
    core_schema_version: int | None = None,
    resolve_current_core: bool = True,
) -> list[StructField]:
    del spark
    return _struct_fields(
        get_resolved_schema_columns(
            schema_name,
            schema_version=schema_version,
            core_schema_key=core_schema_key,
            core_schema_version=core_schema_version,
            resolve_current_core=resolve_current_core,
        )
    )


def get_schema_columns_for_upload(
    spark: SparkSession,
    fallback_schema_name: str,
    file_upload: FileUploadConfig,
) -> list[StructField]:
    del spark
    return _struct_fields(
        get_resolved_schema_columns_for_upload(fallback_schema_name, file_upload)
    )


def get_schema_columns_for_config(
    spark: SparkSession,
    fallback_schema_name: str,
    config: FileConfig,
    *,
    use_upload_pins: bool | None = None,
) -> list[StructField]:
    if use_upload_pins is None:
        use_upload_pins = config.tier in _UPLOAD_PINNED_TIERS
    if not use_upload_pins:
        return get_schema_columns(spark, fallback_schema_name)
    file_upload = try_load_file_upload_config(config.filename_components.id)
    if file_upload is None:
        return get_schema_columns(spark, fallback_schema_name)
    return get_schema_columns_for_upload(spark, fallback_schema_name, file_upload)


def get_schema_columns_by_name(
    spark: SparkSession, schema_name: str
) -> dict[str, StructField]:
    return {field.name: field for field in get_schema_columns(spark, schema_name)}


def render_flag(value: Column, dtype: DataType) -> Column:
    """Render a 0/1 flag per its registered schema type.

    Yes/No when the schema registers the column as a string (the convention DQ
    checks use elsewhere); a straight cast otherwise.
    """
    if isinstance(dtype, StringType):
        return when(value == 1, "Yes").when(value == 0, "No")
    return value.cast(dtype)


def get_schema_column_descriptions(
    spark: SparkSession, schema_name: str
) -> dict[str, str]:
    del spark
    return {
        column.name: column.description
        for column in get_resolved_schema_columns(schema_name)
    }


def get_schema_columns_datahub(spark: SparkSession, schema_name: str) -> list[tuple]:
    del spark
    return _datahub_fields(get_resolved_schema_columns(schema_name))


def get_schema_columns_datahub_for_upload(
    spark: SparkSession,
    fallback_schema_name: str,
    file_upload: FileUploadConfig,
) -> list[tuple]:
    del spark
    return _datahub_fields(
        get_resolved_schema_columns_for_upload(fallback_schema_name, file_upload)
    )


def get_schema_columns_datahub_for_config(
    spark: SparkSession,
    fallback_schema_name: str,
    config: FileConfig,
) -> list[tuple]:
    file_upload = try_load_file_upload_config(config.filename_components.id)
    if file_upload is None:
        return get_schema_columns_datahub(spark, fallback_schema_name)
    return get_schema_columns_datahub_for_upload(
        spark, fallback_schema_name, file_upload
    )


def get_primary_key(spark: SparkSession, schema_name: str) -> str:
    del spark
    return primary_key_name(get_resolved_schema_columns(schema_name), schema_name)


def get_primary_key_for_upload(
    spark: SparkSession,
    fallback_schema_name: str,
    file_upload: FileUploadConfig,
) -> str:
    del spark
    columns = get_resolved_schema_columns_for_upload(fallback_schema_name, file_upload)
    schema_name = file_upload.schema_key or fallback_schema_name
    return primary_key_name(columns, schema_name)


def get_primary_key_for_config(
    spark: SparkSession,
    fallback_schema_name: str,
    config: FileConfig,
    *,
    use_upload_pins: bool | None = None,
) -> str:
    if use_upload_pins is None:
        use_upload_pins = config.tier in _UPLOAD_PINNED_TIERS
    if not use_upload_pins:
        return get_primary_key(spark, fallback_schema_name)
    file_upload = try_load_file_upload_config(config.filename_components.id)
    if file_upload is None:
        return get_primary_key(spark, fallback_schema_name)
    return get_primary_key_for_upload(spark, fallback_schema_name, file_upload)


def get_partition_columns(spark: SparkSession, schema_name: str) -> list[str]:
    del spark
    return [
        column.name
        for column in sorted(
            (
                column
                for column in get_resolved_schema_columns(schema_name)
                if column.partition_order is not None and column.partition_order > 0
            ),
            key=lambda column: column.partition_order,
        )
    ]


def construct_schema_name_for_tier(schema_name: str, tier: DataTier = None) -> str:
    if tier is not None and tier in [
        DataTier.SILVER,
        DataTier.STAGING,
        DataTier.MANUAL_REJECTED,
    ]:
        return f"{schema_name.lower()}_{tier.value}"
    return schema_name.lower()


def construct_full_table_name(schema_name: str, table_name: str) -> str:
    return f"{schema_name.lower()}.{table_name.lower()}"
