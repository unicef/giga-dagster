from delta.tables import DeltaTable
from pyspark.sql import functions as f
from pyspark.sql import types

from src.settings import settings
from src.utils.spark import get_spark_session


def main():
    spark = get_spark_session()

    df = spark.read.csv(
        f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/master/BEN_school_geolocation_coverage_master.csv",
        inferSchema=True,
        header=True,
    )

    cols_to_int = [
        "school_data_collection_year",
        "connectivity_govt_collection_year",
        "school_establishment_year",
    ]
    cols_to_timestamp = [
        "school_location_ingestion_timestamp",
        "connectivity_govt_ingestion_timestamp",
        "connectivity_RT_ingestion_timestamp",
    ]

    for col in cols_to_int:
        df = df.withColumn(col, f.col(col).cast(types.IntegerType()))
    for col in cols_to_timestamp:
        df = df.withColumn(col, f.col(col).cast(types.TimestampType()))

    spark.sql("CREATE SCHEMA IF NOT EXISTS school_data_v2").show()

    dt = (
        DeltaTable.createIfNotExists(spark)
        .tableName("school_data_v2.BEN")
        .location(f"{settings.AZURE_BLOB_CONNECTION_URI}/gold/delta-tables-v2/BEN")
        .addColumns(df.schema)
        .execute()
    )
    dt.alias("source").merge(
        source=df.alias("new"),
        condition=f.expr("source.school_id_giga = new.school_id_giga"),
    ).whenNotMatchedInsertAll().execute()


if __name__ == "__main__":
    main()
