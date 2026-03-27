from datetime import datetime
from typing import ClassVar

from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DateTypeClass,
    NumberTypeClass,
    StringTypeClass,
)
from models import TypeMapping, TypeMappings
from pydantic_settings import BaseSettings
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)


class Constants(BaseSettings):
    UPLOAD_PATH_PREFIX: str = "raw/uploads"
    UPLOAD_METADATA_PATH_PREFIX: str = "raw/upload_metadata"
    datetime_partition_key_format: ClassVar[str] = "%Y-%m-%d-%H:%M"

    connectivity_updates_folder: ClassVar[str] = "raw/connectivity_updates"
    raw_folder: ClassVar[str] = (
        "raw"  # if settings.IN_PRODUCTION else "adls-testing-raw"
    )
    raw_schema_folder: ClassVar[str] = "raw/schema"
    raw_schema_folder_source: ClassVar[str] = "raw/schema"
    bronze_folder: ClassVar[str] = "bronze"
    silver_folder: ClassVar[str] = "silver"
    gold_folder: ClassVar[str] = "gold"
    dq_results_folder: ClassVar[str] = "data-quality-results"
    staging_folder: ClassVar[str] = "staging"

    dq_passed_folder: ClassVar[str] = "staging/pending-review"
    staging_approved_folder: ClassVar[str] = "staging/approved"
    archive_manual_review_rejected_folder: ClassVar[str] = (
        "archive/manual-review-rejected"
    )
    gold_source_folder: ClassVar[str] = "updated_master_schema"
    adhoc_master_updates_source_folder: ClassVar[str] = (
        "updated_master_schema/master_updates"
    )
    qos_source_folder: ClassVar[str] = "gold/qos"
    qos_raw_source_folder: ClassVar[str] = "gold/qos-raw"
    error_folder: ClassVar[str] = "error"

    # can't set infinite, just set to a value most likely beyond the extinction of the human race
    school_master_retention_period: ClassVar[str] = "interval 1000000 weeks"
    qos_retention_period: ClassVar[str] = "interval 90 days"

    TYPE_MAPPINGS: TypeMappings = TypeMappings(
        string=TypeMapping(
            native=str,
            pyspark=StringType,
            datahub=StringTypeClass,
        ),
        integer=TypeMapping(
            native=int,
            pyspark=IntegerType,
            datahub=NumberTypeClass,
        ),
        long=TypeMapping(
            native=int,
            pyspark=LongType,
            datahub=NumberTypeClass,
        ),
        float=TypeMapping(
            native=float,
            pyspark=FloatType,
            datahub=NumberTypeClass,
        ),
        double=TypeMapping(
            native=float,
            pyspark=DoubleType,
            datahub=NumberTypeClass,
        ),
        timestamp=TypeMapping(
            native=datetime,
            pyspark=TimestampType,
            datahub=DateTypeClass,
        ),
        boolean=TypeMapping(
            native=bool,
            pyspark=BooleanType,
            datahub=BooleanTypeClass,
        ),
    )


constants = Constants()
