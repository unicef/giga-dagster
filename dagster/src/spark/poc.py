import pyspark.pandas as pd

from src.settings import AZURE_BLOB_CONNECTION_URI
from src.spark.spark import get_spark_session

spark = get_spark_session()


def create_test_table():
    spark.sql(
        f"""
    CREATE OR REPLACE TABLE gold.delta_test (
        id LONG
    )
    USING DELTA
    LOCATION '{AZURE_BLOB_CONNECTION_URI}/fake-gold/delta-spark'
    """
    )
    df = pd.DataFrame({"id": range(5)})
    data = df.to_spark(None)
    data.write.format("delta").mode("overwrite").saveAsTable("gold.delta_test")


def load_test_table():
    df = spark.table("gold.delta_test")
    df.show()
    return df
