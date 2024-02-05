import uuid

import h3
import json
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.window import Window

from src.settings import settings
from src.spark.check_functions import (
    get_decimal_places_udf,
    has_similar_name_udf,
    is_within_country_udf,
)
from src.spark.config_expectations import (
    CONFIG_NONEMPTY_COLUMNS_CRITICAL,
    CONFIG_NONEMPTY_COLUMNS_WARNING,
    CONFIG_UNIQUE_COLUMNS,
    CONFIG_UNIQUE_SET_COLUMNS,
    CONFIG_VALUES_RANGE,
    CONFIG_VALUES_RANGE_PRIO,
)

# check unique columns (DUPLICATES) (CONFIG_UNIQUE_COLUMNS)
#   "df.filter(F.expr(NOT (count(1) OVER (PARTITION BY giga_id_school unspecifiedframe$()) <= 1)))"
# critical non null columns (CONFIG_NONEMPTY_COLUMNS_CRITICAL)
#   "df.filter(F.expr(NOT (school_name IS NOT NULL)))"
# range checks  CONFIG_VALUES_RANGE
#   "df.filter(F.expr((longitude IS NOT NULL) AND (NOT ((longitude >= -180) AND (longitude <= 180)))))"
# domain checks CONFIG_VALUES_OPTIONS

## CUSTOM EXPECTATIONS ##
# Geospatial (is_within_country)
# not SIMILAR (school name)
# check for five decimal places
# type?? probably not
# unique set columns (CONFIG_UNIQUE_SET_COLUMNS)
#   "df.filter(F.expr(NOT (count(1) OVER (PARTITION BY struct(school_id, school_name, education_level, latitude, longitude)

# Duplicate Checks (test_duplicate_ config? :hmm:)
def uniqueness_checks(df, CONFIG_COLUMN_LIST):
    for column in CONFIG_COLUMN_LIST:
        column_name = f"duplicate_{column}"
        df = df.withColumn(
            column_name,
            f.when(
                f.count(f"{column}").over(Window.partitionBy(f"{column}")) > 1,
                1,
            ).otherwise(0),
        )
    return df


if __name__ == "__main__":
    from src.utils.spark import get_spark_session
    # 
    file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/bronze/school-geolocation-data/BLZ_school-geolocation_gov_20230207.csv"
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/adls-testing-raw/_test_BLZ_RAW.csv"
    spark = get_spark_session()
    df = spark.read.csv(file_url, header=True)
    df = df.sort("school_name").limit(10)
    df = uniqueness_checks(df, ["school_id","school_name", "internet_availability"])
    # df = df.limit(10)
    df.show()
    
    
    # import json

    # json_file_path =  "src/spark/ABLZ_school-geolocation_gov_20230207_test.json"
    # # # json_file_path =  'C:/Users/RenzTogonon/Downloads/ABLZ_school-geolocation_gov_20230207_test (4).json'

    # with open(json_file_path, 'r') as file:
    #     data = json.load(file)

    # dq_passed_rows(df, data).show()
