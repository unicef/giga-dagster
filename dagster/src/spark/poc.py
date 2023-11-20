import string
from random import choice, randint
from uuid import uuid4

import pyspark.pandas as pd

from src.settings import AZURE_BLOB_CONNECTION_URI
from src.spark.spark import get_spark_session

spark = get_spark_session()


def create_test_table():
    spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
    spark.sql(
        f"""
    CREATE TABLE IF NOT EXISTS gold.delta_test (
        giga_id_school STRING,
        school_id LONG,
        name STRING,
        lat DOUBLE,
        long DOUBLE,
        education_level STRING,
        education_level_regional STRING,
        school_type STRING,
        connectivity STRING,
        coverage_availability STRING
    )
    USING DELTA
    LOCATION '{AZURE_BLOB_CONNECTION_URI}/fake-gold/delta-spark'
    """
    )
    df = pd.DataFrame(
        {
            "giga_id_school": [str(uuid4()) for _ in range(10)],
            "school_id": [randint(10000000, 11000000) for _ in range(10)],
            "name": [f"School {string.ascii_uppercase[i]}" for i in range(10)],
            "lat": [float(randint(0, 100)) for _ in range(10)],
            "long": [float(randint(0, 100)) for _ in range(10)],
            "education_level": [None for _ in range(10)],
            "education_level_regional": [None for _ in range(10)],
            "school_type": [choice(["Estadual", "Municipal"]) for _ in range(10)],
            "connectivity": [choice(["YES", "NO"]) for _ in range(10)],
            "coverage_availability": [choice(["YES", "NO"]) for _ in range(10)],
        }
    )
    data = df.to_spark(None)
    data.write.format("delta").mode("overwrite").saveAsTable("gold.delta_test")


def load_test_table():
    df = spark.table("gold.delta_test")
    df.show()
    return df
