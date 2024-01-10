import uuid

import h3
import json
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.window import Window

from src.settings import settings

def rename_raw_columns(df):
    column_mapping = {
        # raw, delta_col, dtype
        ("school_id", "school_id_gov"), 
        ("school_name", "school_name"), 
        ("school_id_gov_type", "school_id_gov_type"), 
        ("school_establishment_year", "school_establishment_year"), 
        ("latitude", "latitude"), 
        ("longitude", "longitude"), 
        ("education_level", "education_level"), 
        ("education_level_govt", "education_level_govt"), 
        ("internet_availability", "connectivity_govt"), 
        ("connectivity_govt_ingestion_timestamp", "connectivity_govt_ingestion_timestamp"),
        ("internet_speed_mbps", "download_speed_govt"), 
        ("download_speed_contracted", "download_speed_contracted"), 
        ("internet_type", "connectivity_type_govt"), 
        ("admin1", "admin1"), 
        ("admin2", "admin2"), 
        ("school_region", "school_area_type"), 
        ("school_funding_type", "school_funding_type"),
        ("computer_count", "num_computers"), 
        ("desired_computer_count", "num_computers_desired"), 
        ("teacher_count", "num_teachers"), 
        ("adm_personnel_count", "num_adm_personnel"), 
        ("student_count", "num_students"), 
        ("classroom_count", "num_classrooms"), 
        ("num_latrines", "num_latrines"), 
        ("computer_lab", "computer_lab"), 
        ("electricity", "electricity_availability"), 
        ("electricity_type", "electricity_type"), 
        ("water", "water_availability"), 
        ("address", "school_address"), 
        ("school_data_source", "school_data_source"), 
        ("school_data_collection_year", "school_data_collection_year"), 
        ("school_data_collection_modality", "school_data_collection_modality"),
        ("is_open", "is_school_open"),
    }

    bronze_columns = [delta_col for _, delta_col in column_mapping]

    # Iterate over mapping set and perform actions
    for raw_col, delta_col in column_mapping:
    # Check if the raw column exists in the DataFrame
        if raw_col in df.columns:
        # If it exists in raw, rename it to the delta column
            df = df.withColumnRenamed(raw_col, delta_col)
        # If it doesn't exist in both, create a null column placeholder with the delta column name
        elif delta_col in df.columns:
            pass
        else:
            df = df.withColumn(delta_col, f.lit(None))
    
    df = df.select(*bronze_columns)
    
    return df

if __name__ == "__main__":
    from src.utils.spark import get_spark_session
    # 
    file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/bronze/school-geolocation-data/BLZ_school-geolocation_gov_20230207.csv"
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/adls-testing-raw/_test_BLZ_RAW.csv"
    spark = get_spark_session()
    df = spark.read.csv(file_url, header=True)
    df = rename_raw_columns(df)
    df.show()
    # df.show()
    # df = df.limit(10)
    
    
    # import json

    # json_file_path =  "src/spark/ABLZ_school-geolocation_gov_20230207_test.json"
    # # # json_file_path =  'C:/Users/RenzTogonon/Downloads/ABLZ_school-geolocation_gov_20230207_test (4).json'

    # with open(json_file_path, 'r') as file:
    #     data = json.load(file)

    # dq_passed_rows(df, data).show()