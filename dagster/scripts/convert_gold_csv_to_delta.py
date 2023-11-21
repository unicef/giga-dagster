import pandas as pd

from src.resources.adls_file_client import ADLSFileClient
from src.settings import AZURE_BLOB_CONNECTION_URI
from src.spark.spark import get_spark_session

spark = get_spark_session()
client = ADLSFileClient()


def main():
    df = client.download_from_adls("gold/BEN_school_geolocation_coverage_master.csv")
    df = df.astype(pd.StringDtype())

    columns_convert_to_float = ["connectivity_speed", "latency_connectivity"]
    columns_convert_to_double = [
        "lat",
        "lon",
        "fiber_node_distance",
        "microwave_node_distance",
        "nearest_school_distance",
        "nearest_UMTS_distance",
        "nearest_GSM_distance",
    ]
    columns_convert_to_int = [
        "num_computers",
        "num_teachers",
        "num_students",
        "num_classroom",
        "nearest_GSM_id",
        "schools_within_1km",
        "schools_within_2km",
        "schools_within_3km",
        "schools_within_10km",
    ]
    columns_convert_to_long = [
        "nearest_LTE_id",
        "nearest_UMTS_id",
        "nearest_GSM_id",
        "pop_within_1km",
        "pop_within_2km",
        "pop_within_3km",
        "pop_within_10km",
    ]

    for col in columns_convert_to_float:
        df[col] = df[col].astype(pd.Float32Dtype())
    for col in columns_convert_to_double:
        df[col] = df[col].astype(pd.Float64Dtype())
    for col in columns_convert_to_int:
        df[col] = df[col].astype(pd.Float32Dtype()).astype(pd.Int32Dtype())
    for col in columns_convert_to_long:
        df[col] = df[col].astype(pd.Float32Dtype()).astype(pd.Int64Dtype())

    df = spark.createDataFrame(df)

    spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

    spark.sql(
        f"""
    CREATE TABLE IF NOT EXISTS gold.BEN_school_master (
        giga_id_school STRING NOT NULL,
        school_id STRING,
        name STRING,
        lat DOUBLE NOT NULL,
        lon DOUBLE NOT NULL,
        education_level STRING,
        education_level_regional STRING,
        school_type STRING,
        connectivity STRING,
        connectivity_speed FLOAT,
        type_connectivity STRING,
        coverage_availability STRING,
        coverage_type STRING,
        latency_connectivity FLOAT,
        admin1 STRING,
        admin2 STRING,
        admin3 STRING,
        admin4 STRING,
        school_region STRING,
        num_computers INT,
        num_teachers INT,
        num_students INT,
        num_classroom INT,
        computer_availability STRING,
        computer_lab STRING,
        electricity STRING,
        water STRING,
        address STRING,
        fiber_node_distance DOUBLE,
        microwave_node_distance DOUBLE,
        nearest_school_distance DOUBLE,
        schools_within_1km INT,
        schools_within_2km INT,
        schools_within_3km INT,
        schools_within_10km INT,
        nearest_LTE_id LONG,
        nearest_UMTS_id LONG,
        nearest_UMTS_distance DOUBLE,
        nearest_GSM_id LONG,
        nearest_GSM_distance DOUBLE,
        pop_within_1km LONG,
        pop_within_2km LONG,
        pop_within_3km LONG,
        pop_within_10km LONG
    )
    USING DELTA
    LOCATION '{AZURE_BLOB_CONNECTION_URI}/gold/BEN-school_geolocation_coverage-master'
    """
    )

    df.write.format("delta").mode("overwrite").saveAsTable("gold.BEN_school_master")


if __name__ == "__main__":
    main()
