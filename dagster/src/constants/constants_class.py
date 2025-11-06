from datetime import datetime

from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DateTypeClass,
    NumberTypeClass,
    StringTypeClass,
)
from models import TypeMapping, TypeMappings
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from src.settings import settings


class Constants:
    UPLOAD_PATH_PREFIX: str = f"{settings.LAKEHOUSE_PATH}/raw/uploads"
    datetime_partition_key_format = "%Y-%m-%d-%H:%M"

    connectivity_updates_folder = f"{settings.LAKEHOUSE_PATH}/raw/connectivity_updates"
    raw_folder = f"{settings.LAKEHOUSE_PATH}/raw"  # if settings.IN_PRODUCTION else "adls-testing-raw"
    raw_schema_folder = f"{settings.LAKEHOUSE_PATH}/raw/schema"
    raw_schema_folder_source = "raw/schema"
    bronze_folder = f"{settings.LAKEHOUSE_PATH}/bronze"
    silver_folder = f"{settings.LAKEHOUSE_PATH}/silver"
    gold_folder = f"{settings.LAKEHOUSE_PATH}/gold"
    dq_results_folder = f"{settings.LAKEHOUSE_PATH}/data-quality-results"
    staging_folder = f"{settings.LAKEHOUSE_PATH}/staging"

    dq_passed_folder = f"{settings.LAKEHOUSE_PATH}/staging/pending-review"
    staging_approved_folder = f"{settings.LAKEHOUSE_PATH}/staging/approved"
    archive_manual_review_rejected_folder = (
        f"{settings.LAKEHOUSE_PATH}/archive/manual-review-rejected"
    )
    gold_source_folder = f"{settings.LAKEHOUSE_PATH}/updated_master_schema"
    adhoc_master_updates_source_folder = (
        f"{settings.LAKEHOUSE_PATH}/updated_master_schema/master_updates"
    )
    qos_source_folder = f"{settings.LAKEHOUSE_PATH}/gold/qos"
    qos_raw_source_folder = f"{settings.LAKEHOUSE_PATH}/gold/qos-raw"
    error_folder = f"{settings.LAKEHOUSE_PATH}/error"

    # can't set infinite, just set to a value most likely beyond the extinction of the human race
    school_master_retention_period = "interval 1000000 weeks"
    qos_retention_period = "interval 90 days"

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
