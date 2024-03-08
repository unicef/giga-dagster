from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    TimestampType,
)

from .base import BaseSchema


class SchoolMasterSchema(BaseSchema):
    @property
    def columns(self):
        return [
            StructField("cellular_coverage_availability", StringType(), True),
            StructField("cellular_coverage_type", StringType(), True),
            StructField("fiber_node_distance", DoubleType(), True),
            StructField("microwave_node_distance", DoubleType(), True),
            StructField("schools_within_1km", IntegerType(), True),
            StructField("schools_within_2km", IntegerType(), True),
            StructField("schools_within_3km", IntegerType(), True),
            StructField("nearest_NR_distance", DoubleType(), True),
            StructField("nearest_LTE_distance", DoubleType(), True),
            StructField("nearest_UMTS_distance", DoubleType(), True),
            StructField("nearest_GSM_distance", DoubleType(), True),
            StructField("pop_within_1km", LongType(), True),
            StructField("pop_within_2km", LongType(), True),
            StructField("pop_within_3km", LongType(), True),
            StructField("connectivity_govt_collection_year", IntegerType(), True),
            StructField("connectivity_govt", StringType(), True),
            StructField("school_id_giga", StringType(), False),
            StructField("school_id_govt", StringType(), False),
            StructField("school_name", StringType(), False),
            StructField("school_establishment_year", IntegerType(), True),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("education_level", StringType(), False),
            StructField("download_speed_contracted", DoubleType(), True),
            StructField("connectivity_type_govt", StringType(), True),
            StructField("admin1", StringType(), False),
            StructField("admin1_id_giga", StringType(), True),
            StructField("admin2", StringType(), False),
            StructField("admin2_id_giga", StringType(), True),
            StructField("school_area_type", StringType(), True),
            StructField("school_funding_type", StringType(), True),
            StructField("num_computers", IntegerType(), True),
            StructField("num_computers_desired", IntegerType(), True),
            StructField("num_teachers", IntegerType(), True),
            StructField("num_adm_personnel", IntegerType(), True),
            StructField("num_students", IntegerType(), True),
            StructField("num_classrooms", IntegerType(), True),
            StructField("num_latrines", IntegerType(), True),
            StructField("computer_lab", StringType(), True),
            StructField("electricity_availability", StringType(), True),
            StructField("electricity_type", StringType(), True),
            StructField("water_availability", StringType(), True),
            StructField("school_data_source", StringType(), True),
            StructField("school_data_collection_year", IntegerType(), True),
            StructField("school_data_collection_modality", StringType(), True),
            StructField("connectivity_govt_ingestion_timestamp", TimestampType(), True),
            StructField("school_location_ingestion_timestamp", TimestampType(), True),
            StructField("disputed_region", StringType(), True),
            StructField("connectivity", StringType(), True),
            StructField("connectivity_RT", StringType(), True),
            StructField("connectivity_RT_datasource", StringType(), True),
            StructField("connectivity_RT_ingestion_timestamp", TimestampType(), True),
        ]


school_master = SchoolMasterSchema()
