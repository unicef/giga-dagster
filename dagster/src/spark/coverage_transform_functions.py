from pyspark import sql
from pyspark.sql import functions as f
from pyspark.sql.types import NullType

from src.settings import settings
from src.spark.config_expectations import config
from src.utils.schema import get_schema_columns

# General Transform Components


def coverage_column_filter(df: sql.DataFrame, config_columns_to_keep: list[str]):
    df = df.select(*config_columns_to_keep)
    return df


def coverage_row_filter(df: sql.DataFrame):
    df = df.filter(f.col("school_id_giga").isNotNull())
    return df


# FB Transform Components


def fb_percent_to_boolean(df: sql.DataFrame):
    df = df.withColumn("2G_coverage", f.col("percent_2G") > 0)
    df = df.withColumn("3G_coverage", f.col("percent_3G") > 0)
    df = df.withColumn("4G_coverage", f.col("percent_4G") > 0)

    df = df.drop("percent_2G")
    df = df.drop("percent_3G")
    df = df.drop("percent_4G")
    return df


def fb_transforms(fb: sql.DataFrame):
    spark = fb.sparkSession

    # fb
    fb = fb_percent_to_boolean(fb)
    fb = coverage_column_filter(fb, config.FB_COLUMNS)
    fb = coverage_row_filter(fb)

    # coverage availability and type columns
    fb = fb.withColumn(
        "cellular_coverage_type",
        (
            f.when(f.col("5G_coverage"), f.lit("5G"))
            .when(f.col("4G_coverage"), f.lit("4G"))
            .when(f.col("3G_coverage"), f.lit("3G"))
            .when(f.col("2G_coverage"), f.lit("2G"))
            .otherwise(f.lit("no coverage"))
        ),
    )
    fb = fb.withColumn(
        "cellular_coverage_availability",
        f.when(f.col("cellular_coverage_type") == "no coverage", "no").otherwise("yes"),
    )

    # add cov schema
    cov_columns = get_schema_columns(spark, "school_coverage")
    columns_to_add = {
        col.name: f.lit(None).cast(NullType())
        for col in cov_columns
        if col.name not in fb.columns
    }
    fb = fb.withColumns(columns_to_add)

    # remove Xg_coverage_{source} because they are accounted for in cellular_coverage_type
    fb = fb.drop("2G_coverage", "3G_coverage", "4G_coverage", "5G_coverage")

    return fb


def fb_coverage_merge(fb: sql.DataFrame, cov: sql.DataFrame):
    # add suffixes
    for col in fb.columns:
        if col != "school_id_giga":  # add suffix except join key
            fb = fb.withColumnRenamed(col, col + "_fb")

    # outer join
    cov_stg = cov.join(fb, on="school_id_giga", how="outer")

    # coalesce with updated values
    for col in cov.columns:
        if (
            col not in config.COV_COLUMN_MERGE_LOGIC and col != "school_id_giga"
        ):  # coverage availability and type are merged differently
            cov_stg = cov_stg.withColumn(col, f.coalesce(col + "_fb", col))
            cov_stg = cov_stg.drop(col + "_fb")

    # harmonize specific columns
    cov_stg = cov_stg.withColumn(
        "cellular_coverage_type",
        f.expr(
            "CASE "
            "WHEN cellular_coverage_type = '5G' OR cellular_coverage_type_fb = '5G' THEN '5G' "
            "WHEN cellular_coverage_type = '4G' OR cellular_coverage_type_fb = '4G' THEN '4G' "
            "WHEN cellular_coverage_type = '3G' OR cellular_coverage_type_fb = '3G' THEN '3G' "
            "WHEN cellular_coverage_type = '2G' OR cellular_coverage_type_fb = '4G' THEN '2G' "
            "ELSE 'no coverage' "
            "END",
        ),
    )
    cov_stg = cov_stg.drop("cellular_coverage_type_fb")
    cov_stg = cov_stg.withColumn(
        "cellular_coverage_availability",
        f.expr(
            "CASE "
            "WHEN cellular_coverage_type = 'no coverage' then 'no' "
            "ELSE 'yes' "
            "END",
        ),
    )
    cov_stg = cov_stg.drop("cellular_coverage_availability_fb")

    return cov_stg


# ITU Transform Components


def itu_binary_to_boolean(df: sql.DataFrame):
    df = df.withColumn("2G_coverage", f.col("2g_mobile_coverage") == 1)
    df = df.withColumn("3G_coverage", f.col("3g_mobile_coverage") == 1)
    df = df.withColumn("4G_coverage", f.col("4g_mobile_coverage") == 1)
    df = df.withColumn("5G_coverage", f.col("5g_mobile_coverage") == 1)

    df = df.drop("2g_mobile_coverage")
    df = df.drop("3g_mobile_coverage")
    df = df.drop("4g_mobile_coverage")
    df = df.drop("5g_mobile_coverage")
    return df


def itu_lower_columns(df: sql.DataFrame):
    for col_name in config.ITU_COLUMNS_TO_RENAME:
        df = df.withColumnRenamed(col_name, col_name.lower())
    return df


def itu_transforms(itu: sql.DataFrame):
    # fb
    itu = itu_binary_to_boolean(itu)
    itu = itu_lower_columns(itu)  # should i remove given column mapping portal?
    itu = coverage_column_filter(itu, config.ITU_COLUMNS)
    itu = coverage_row_filter(itu)

    # rename columns to match legacy names
    itu = itu.withColumnsRenamed(
        {'fiber_node_dist': 'fiber_node_distance',
         '5g_cell_site_dist': 'nearest_NR_distance',
         '4g_cell_site_dist': 'nearest_LTE_distance',
         '3g_cell_site_dist': 'nearest_UMTS_distance',
         '2g_cell_site_dist': 'nearest_GSM_distance'
         }
    )

    # coverage availability and type columns
    itu = itu.withColumn(
        "cellular_coverage_type",
        (
            f.when(f.col("5G_coverage"), f.lit("5G"))
            .when(f.col("4G_coverage"), f.lit("4G"))
            .when(f.col("3G_coverage"), f.lit("3G"))
            .when(f.col("2G_coverage"), f.lit("2G"))
            .otherwise("no coverage")
        ),
    )
    itu = itu.withColumn(
        "cellular_coverage_availability",
        f.when(f.col("cellular_coverage_type") == "no coverage", "no").otherwise("yes"),
    )

    # add cov schema
    cov_columns = get_schema_columns(itu.sparkSession, "school_coverage")
    columns_to_add = {
        col.name: f.lit(None).cast(NullType())
        for col in cov_columns
        if col.name not in itu.columns
    }
    itu = itu.withColumns(columns_to_add)

    # remove Xg_coverage_{source} because they are accounted for in cellular_coverage_type
    itu = itu.drop("2G_coverage", "3G_coverage", "4G_coverage", "5G_coverage")

    return itu


def itu_coverage_merge(itu: sql.DataFrame, cov: sql.DataFrame):
    # add suffixes
    for col in itu.columns:
        if col != "school_id_giga":  # add suffix except join key
            itu = itu.withColumnRenamed(col, col + "_itu")

    # outer join
    cov_stg = cov.join(itu, on="school_id_giga", how="outer")

    # coalesce with updated values
    for col in cov.columns:
        if (
            col not in config.COV_COLUMN_MERGE_LOGIC and col != "school_id_giga"
        ):  # coverage availability and type are merged differently
            cov_stg = cov_stg.withColumn(col, f.coalesce(col + "_itu", col))
            cov_stg = cov_stg.drop(col + "_itu")

    # harmonize specific columns
    cov_stg = cov_stg.withColumn(
        "cellular_coverage_type",
        f.expr(
            "CASE "
            "WHEN cellular_coverage_type = '5G' OR cellular_coverage_type_itu = '5G' THEN '5G' "
            "WHEN cellular_coverage_type = '4G' OR cellular_coverage_type_itu = '4G' THEN '4G' "
            "WHEN cellular_coverage_type = '3G' OR cellular_coverage_type_itu = '3G' THEN '3G' "
            "WHEN cellular_coverage_type = '2G' OR cellular_coverage_type_itu = '4G' THEN '2G' "
            "ELSE 'no coverage' "
            "END",
        ),
    )
    cov_stg = cov_stg.drop("cellular_coverage_type_itu")
    cov_stg = cov_stg.withColumn(
        "cellular_coverage_availability",
        f.expr(
            "CASE "
            "WHEN cellular_coverage_type = 'no coverage' then 'no' "
            "ELSE 'yes' "
            "END",
        ),
    )
    cov_stg = cov_stg.drop("cellular_coverage_availability_itu")

    return cov_stg


if __name__ == "__main__":

    def test():
        from src.utils.spark import get_spark_session

        #
        file_url_fb = f"{settings.AZURE_BLOB_CONNECTION_URI}/raw/school_geolocation_coverage_data/bronze/coverage_data/UZB_school-coverage_meta_20230927-091814.csv"
        # file_url_itu = f"{settings.AZURE_BLOB_CONNECTION_URI}/raw/school_geolocation_coverage_data/bronze/coverage_data/UZB_school-coverage_itu_20230927-091823.csv"
        file_url_cov = f"{settings.AZURE_BLOB_CONNECTION_URI}/raw/school_geolocation_coverage_data/silver/coverage_data/UZB_school-coverage_master.csv"
        # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/adls-testing-raw/_test_BLZ_RAW.csv"
        spark = get_spark_session()
        fb = spark.read.csv(file_url_fb, header=True)
        # itu = spark.read.csv(file_url_itu, header=True)
        cov = spark.read.csv(file_url_cov, header=True)

        # itu.show()
        ## CONFORM TEST FILES TO PROPER SCHEMA
        cov = cov.withColumnRenamed("giga_id_school", "school_id_giga")
        cov = cov.withColumnRenamed(
            "coverage_availability", "cellular_coverage_availability"
        )
        cov = cov.withColumnRenamed("coverage_type", "cellular_coverage_type")
        cov = cov.withColumn("nearest_NR_id", f.lit(None))
        cov = cov.withColumn("nearest_NR_distance", f.lit(None))
        cov_columns = [
            "school_id_giga",
            "cellular_coverage_availability",
            "cellular_coverage_type",
            "fiber_node_distance",
            "microwave_node_distance",
            "schools_within_1km",
            "schools_within_2km",
            "schools_within_3km",
            "nearest_NR_distance",
            "nearest_LTE_distance",
            "nearest_UMTS_distance",
            "nearest_GSM_distance",
            "pop_within_1km",
            "pop_within_2km",
            "pop_within_3km",
            "pop_within_10km",
            "nearest_school_distance",
            "schools_within_10km",
            "nearest_NR_id",
            "nearest_LTE_id",
            "nearest_UMTS_id",
            "nearest_GSM_id",
        ]
        cov = cov.select(cov_columns)
        # itu.show()
        fb = fb.withColumnRenamed("giga_id_school", "school_id_giga")
        fb = fb.select("school_id_giga", "percent_2G", "percent_3G", "percent_4G")
        fb.show()
        # df = row_level_checks(
        #     fb,
        #     "coverage_fb",
        #     "UZB",
        # )  # dataset plugged in should conform to updated schema! rename if necessary
        # df.show()

        # df = aggregate_report_spark_df(spark=spark, df=df)
        # df.show()

        # _json = aggregate_report_json(df, itu)
        # print(_json)

        # ## filter to one entry for testing
        fb = fb.filter(
            f.col("school_id_giga") == "a8b4968c-fcb2-31fd-83b1-01b2c48625f3"
        )
        fb.show()
        # itu = itu.filter(f.col("school_id_giga") == "a8b4968c-fcb2-31fd-83b1-01b2c48625f3")
        cov = cov.filter(
            f.col("school_id_giga") == "a8b4968c-fcb2-31fd-83b1-01b2c48625f3"
        )
        cov.show()

        # ## DAGSTER WORKFLOW ##

        # ## TRANSFORM STEP
        # # FB
        fb = fb_transforms(fb)
        fb = fb.withColumn("cellular_coverage_type", f.lit("3G"))
        fb.show()
        df1 = fb_coverage_merge(fb, cov)  # NEED SILVER COVERAGE INPUT
        print("Merged FB and (Silver) Coverage Dataset")
        df1.show()

        # # ITU
        # itu = itu_transforms(itu)
        # itu.show()
        # cov.show()
        # df2 = itu_coverage_merge(itu, cov)  # NEED SILVER COVERAGE INPUT
        # print("Merged ITU and (Silver) Coverage Dataset")
        # df2.show()

        # from src.spark.data_quality_tests import (row_level_checks, aggregate_report_sparkdf, aggregate_report_json)

    test()
