from src.data_quality_checks.name_similarity import has_similar_name
from src.settings import settings

if __name__ == "__main__":
    from src.utils.spark import get_spark_session

    spark = get_spark_session()
    #
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/bronze/school-geolocation-data/BLZ_school-geolocation_gov_20230207.csv"
    file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/master/GHA_school_geolocation_coverage_master.csv"
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/adls-testing-raw/_test_BLZ_RAW.csv"
    df_bronze = spark.read.csv(file_url, header=True)
    df_bronze = df_bronze.sort("school_name").limit(1000)
    df_bronze = df_bronze.withColumnRenamed("school_id_gov", "school_id_govt")
    df_bronze = df_bronze.withColumnRenamed("num_classroom", "num_classrooms")
    # df = domain_checks(df_bronze, VALUES_DOMAIN_MASTER)
    df_test = has_similar_name(df_bronze)
    df_test.show()
    # df_bronze = df_bronze.withColumn("test", f.lower(f.col("admin2_id_giga")))

    # row_level_checks(df, dataset_type, country_code_iso3)
    # df = row_level_checks(
    #     df_bronze, "master", "GHA")
    # dataset plugged in should conform to updated schema! rename if necessary
    # df.show()
    # df = df.withColumn("dq_has_critical_error", f.lit(1))

    # df = dq_passed_rows(df, "coverage")
    # df = dq_passed_rows(df, "coverage")
    # df = aggregate_report_sparkdf(df)
    # df.show()
    # df.show()

    # _json = aggregate_report_json(df, df_bronze)
    # print(_json)
