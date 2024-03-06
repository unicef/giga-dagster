from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
)

from .base import BaseSchema


class SchoolReferenceSchema(BaseSchema):
    @property
    def columns(self):
        return [
            StructField("school_id_giga", StringType(), False),
            StructField("pop_within_10km", LongType(), True),
            StructField("nearest_school_distance", DoubleType(), True),
            StructField("schools_within_10km", IntegerType(), True),
            StructField("nearest_NR_id", StringType(), True),
            StructField("nearest_LTE_id", StringType(), True),
            StructField("nearest_UMTS_id", StringType(), True),
            StructField("nearest_GSM_id", StringType(), True),
            StructField("education_level_govt", StringType(), False),
            StructField("download_speed_govt", DoubleType(), True),
            StructField("school_id_govt_type", StringType(), False),
            StructField("school_address", StringType(), True),
            StructField("is_school_open", StringType(), True),
        ]


school_reference = SchoolReferenceSchema()
