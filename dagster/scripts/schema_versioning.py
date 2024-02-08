import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from src.settings import settings
from src.utils.spark import get_spark_session

TABLE_LOCATION = f"{settings.AZURE_BLOB_CONNECTION_URI}/raw_dev/SCHEMA_VER_TEST"
SCHEMA_NAME = "test"
TABLE_NAME = "schema_versioning"
FULL_TABLE_NAME = f"{SCHEMA_NAME}.{TABLE_NAME}"

spark = get_spark_session()

V1_SCHEMA_FIELDS = [
    StructField("id", StringType()),
    StructField("school_id_giga", StringType()),
    StructField("school_id_govt", StringType()),
    StructField("school_name", StringType()),
    StructField("lat", FloatType()),
    StructField("lng", FloatType()),
    StructField("internet_speed_mbps", IntegerType()),
]
V1_SCHEMA = StructType(V1_SCHEMA_FIELDS)

V2_SCHEMA_FIELDS = [
    *V1_SCHEMA_FIELDS,
    StructField("connectivity_RT", FloatType()),
]
V2_SCHEMA = StructType(V2_SCHEMA_FIELDS)


def step_v0():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}").show()
    spark.sql(f"DROP TABLE IF EXISTS {FULL_TABLE_NAME}").show()

    dt = DeltaTable.createOrReplace(spark).tableName(FULL_TABLE_NAME)
    for field in V1_SCHEMA_FIELDS:
        dt.addColumn(field.name, field.dataType)

    (
        dt.property("description", "This is a test table for schema versioning")
        .property("delta.appendOnly", "false")
        .property("delta.enableChangeDataFeed", "true")
        .location(TABLE_LOCATION)
        .execute()
    )


def step_v1():
    pdf = pd.read_csv(settings.BASE_DIR / "scripts" / "fake.csv", encoding="utf-8-sig")
    sdf = spark.createDataFrame(pdf, schema=V1_SCHEMA)
    sdf.write.format("delta").mode("append").saveAsTable(FULL_TABLE_NAME)


def step_v2():
    pdf = pd.read_csv(
        settings.BASE_DIR / "scripts" / "fake_v2.csv", encoding="utf-8-sig"
    )
    sdf = spark.createDataFrame(pdf, schema=V2_SCHEMA)

    dt = DeltaTable.replace(spark).tableName(FULL_TABLE_NAME)
    for field in V2_SCHEMA_FIELDS:
        dt.addColumn(field.name, field.dataType)

    (
        dt.property("description", "This is a test table for schema versioning")
        .property("delta.appendOnly", "false")
        .property("delta.enableChangeDataFeed", "true")
        .location(TABLE_LOCATION)
        .execute()
    )

    dt = DeltaTable.forName(spark, FULL_TABLE_NAME)
    (
        dt.alias("master")
        .merge(sdf.alias("new"), "master.id = new.id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


if __name__ == "__main__":
    step_v0()
    step_v1()
    step_v2()
