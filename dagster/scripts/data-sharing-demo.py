from pyspark.sql import functions as f

from src.settings import settings

if __name__ == "__main__":
    from src.utils.spark import get_spark_session

    # TO DO: Change file_url to a cloned one. DON'T USE ACTUAL GOLD DATA
    file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/gold/delta-tables/BEN"
    spark = get_spark_session()
    df = spark.read.format("delta").load(file_url)
    df.select("coverage_availability").show()
    df = df.withColumn(
        "coverage_availability",
        f.expr(
            "CASE "
            "   WHEN coverage_availability = 'Yes' "
            "       THEN 'No' "
            "   WHEN coverage_availability = 'No' "
            "       THEN 'Yes' "
            "ELSE coverage_availability END"
        ),
    )
    df.select("coverage_availability").show()
    # TO DO: Rewrite / send the modified file back to ADLS
