from delta.tables import DeltaTable, SparkSession
from pyspark import sql
from pyspark.sql.functions import expr

from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.spark import get_spark_session
from src.utils.sql import load_sql_template

adls_client = ADLSFileClient()


def upload_spark_dataframe_as_delta_table(
    data: sql.DataFrame, filepath: str, spark: SparkSession
):
    filename = filepath.split("/")[-1]
    country_code = filename.split("_")[0]

    # TODO: Get from context
    schema_name = "school_data_v2"
    create_schema_sql = load_sql_template(
        "create_gold_schema",
        schema_name=schema_name,
    )
    create_table_sql = load_sql_template(
        "create_gold_table",
        schema_name=schema_name,
        table_name=country_code,
        location=f"{settings.AZURE_BLOB_CONNECTION_URI}/{filepath}",
    )
    spark.sql(create_schema_sql)
    spark.sql(create_table_sql)
    data.write.format("delta").mode("overwrite").saveAsTable(
        f"{schema_name}.{country_code}"
    )


def clone_table(spark: SparkSession, filepath, cloned_filepath):
    df = adls_client.download_delta_table_as_spark_dataframe(
        filepath=filepath, spark=spark
    )
    upload_spark_dataframe_as_delta_table(
        data=df, filepath=cloned_filepath, spark=spark
    )


def download_delta_table_as_delta_table(filepath: str, spark: SparkSession):
    adls_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{filepath}"

    return DeltaTable.forPath(spark, f"{adls_path}")


def update_values(adls_path, spark: SparkSession):
    delta_table = download_delta_table_as_delta_table(
        filepath=f"{adls_path}", spark=spark
    )

    delta_table.update(
        condition=expr("coverage_availability == 'No'"),
        set={"coverage_availability": expr("'Yes'")},
    )

    delta_table.delete(condition=expr("connectivity is NULL"))

    return delta_table.toDF().show()


if __name__ == "__main__":
    # CLONE TABLE
    spark = get_spark_session()
    v2_filepath = "gold/delta-tables-v2/BLZ"
    v2_clone = "gold/delta-tables-v2/ZCDFTESTBLZ"
    clone_table(spark=spark, filepath=v2_filepath, cloned_filepath=v2_clone)

    # PRINT TABLE COLUMNS
    delta_table = download_delta_table_as_delta_table(filepath=v2_clone, spark=spark)
    delta_table.toDF().select("connectivity").show()
    delta_table.toDF().select("coverage_availability").show()

    # UPDATE TABLE
    update_values(adls_path=v2_clone, spark=spark)

    # PRINT UPDATED TABLE
    delta_table = download_delta_table_as_delta_table(filepath=v2_clone, spark=spark)
    delta_table.toDF().select("connectivity").show()
    delta_table.toDF().select("coverage_availability").show()
