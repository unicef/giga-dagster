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

TABLE_PROPERTIES = {
    "delta.appendOnly": "false",
    "delta.enableChangeDataFeed": "true",
    # "delta.columnMapping.mode": "name",
    # "delta.minReaderVersion": "2",
    # "delta.minWriterVersion": "5",
}


def step_v0():
    """Generate v1 table"""

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}").show()
    spark.sql(f"DROP TABLE IF EXISTS {FULL_TABLE_NAME}").show()

    dt = DeltaTable.createOrReplace(spark).tableName(FULL_TABLE_NAME)
    for field in V1_SCHEMA_FIELDS:
        dt.addColumn(field.name, field.dataType)

    for key, value in TABLE_PROPERTIES.items():
        dt.property(key, value)

    dt.location(TABLE_LOCATION).execute()


def step_v1():
    """Initial 10 rows"""

    pdf = pd.read_csv(settings.BASE_DIR / "scripts" / "fake.csv", encoding="utf-8-sig")
    sdf = spark.createDataFrame(pdf, schema=V1_SCHEMA)
    sdf.write.format("delta").mode("append").saveAsTable(FULL_TABLE_NAME)


def step_v2():
    """Append 5 new rows + add 1 new column"""

    pdf = pd.read_csv(
        settings.BASE_DIR / "scripts" / "fake_v2.csv", encoding="utf-8-sig"
    )
    sdf = spark.createDataFrame(pdf, schema=V2_SCHEMA)

    sdf.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        FULL_TABLE_NAME
    )


if __name__ == "__main__":
    step_v0()
    step_v1()
    step_v2()
