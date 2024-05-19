from datetime import datetime

from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DateTypeClass,
    NumberTypeClass,
    StringTypeClass,
)
from models import TypeMapping, TypeMappings
from pydantic import BaseSettings
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
    datetime_partition_key_format = "%Y-%m-%d-%H:%M"

    raw_folder = "raw"  # if settings.IN_PRODUCTION else "adls-testing-raw"
    raw_schema_folder = "raw/schema"
    bronze_folder = "bronze"
    silver_folder = "silver"
    gold_folder = "gold"
    dq_results_folder = "data-quality-results"
    staging_folder = "staging"

    dq_passed_folder = "staging/pending-review"
    staging_approved_folder = "staging/approved"
    archive_manual_review_rejected_folder = "archive/manual-review-rejected"
    gold_source_folder = "updated_master_schema"
    adhoc_master_updates_source_folder = "updated_master_schema/master_updates"
    qos_source_folder = "gold/qos"

    # can't set infinite, just set to a value most likely beyond the extinction of the human race
    school_master_retention_period = "interval 1000000000 weeks"
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
