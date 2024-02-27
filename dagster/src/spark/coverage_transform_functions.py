from pyspark.sql import functions as f

from src.settings import settings
from src.spark.config_expectations import (
    CONFIG_COV_COLUMN_MERGE_LOGIC,
    CONFIG_COV_COLUMN_RENAME,
    CONFIG_COV_COLUMNS,
    CONFIG_FB_COLUMNS,
    CONFIG_ITU_COLUMNS,
    CONFIG_ITU_COLUMNS_TO_RENAME,
)

# General Transform Components


def rename_raw_columns(df):
    # Iterate over mapping set and perform actions
    for raw_col, delta_col in CONFIG_COV_COLUMN_RENAME:
        # Check if the raw column exists in the DataFrame
        if raw_col in df.columns:
            # If it exists in raw, rename it to the delta column
            df = df.withColumnRenamed(raw_col, delta_col)

    return df


def coverage_column_filter(df, CONFIG_COLUMNS_TO_KEEP):
    df = df.select(*CONFIG_COLUMNS_TO_KEEP)
    return df


def coverage_row_filter(df):
    df = df.filter(f.col("school_id_giga").isNotNull())
    return df


# FB Transform Components


def fb_percent_to_boolean(df):
    df = df.withColumn("2G_coverage", f.col("percent_2G") > 0)
    df = df.withColumn("3G_coverage", f.col("percent_3G") > 0)
    df = df.withColumn("4G_coverage", f.col("percent_4G") > 0)

    df = df.drop("percent_2G")
    df = df.drop("percent_3G")
    df = df.drop("percent_4G")
    return df


def fb_transforms(fb):
    # fb
    fb = fb_percent_to_boolean(fb)
    fb = coverage_column_filter(fb, CONFIG_FB_COLUMNS)
    fb = coverage_row_filter(fb)

    # coverage availability and type columns
    fb = fb.withColumn(
        "cellular_coverage_type",
        f.expr(
            "CASE "
            "WHEN 4g_coverage IS TRUE THEN '4G' "
            "WHEN 3g_coverage IS TRUE THEN '3G' "
            "WHEN 2g_coverage IS TRUE THEN '2G' "
            "ELSE 'no coverage' "
            "END"
        ),
    )
    fb = fb.withColumn(
        "cellular_coverage_availability",
        f.expr(
            "CASE " 
            "WHEN cellular_coverage_type = 'no coverage' then 'no' "
            "ELSE 'yes' "
            "END"
        ),
    )

    # add cov schema
    for col in CONFIG_COV_COLUMNS:
        if col not in fb.columns:
            fb = fb.withColumn(col, f.lit(None))

    # add suffixes
    for col in fb.columns:
        if col != "school_id_giga":  # add suffix except join key
            fb = fb.withColumnRenamed(col, col + "_fb")

    return fb


def fb_coverage_merge(fb, cov):
    # outer join
    cov_stg = cov.join(fb, on="school_id_giga", how="outer")

    # remove Xg_coverage_{source} because they are accounted for in cellular_coverage_type
    cov_stg = cov_stg.drop("2G_coverage_fb")
    cov_stg = cov_stg.drop("3G_coverage_fb")
    cov_stg = cov_stg.drop("4G_coverage_fb")

    # coalesce with updated values
    for col in cov.columns:
        if (
            col not in CONFIG_COV_COLUMN_MERGE_LOGIC and col != "school_id_giga"
        ):  # coverage availability and type are merged differently
            cov_stg = cov_stg.withColumn(col, f.coalesce(col + "_fb", col))
            cov_stg = cov_stg.drop(col + "_fb")

    # harmonize specific columns
    cov_stg = cov_stg.withColumn(
        "cellular_coverage_type",
        f.expr(
            "CASE "
            "WHEN cellular_coverage_type = '4G' OR cellular_coverage_type_fb = '4G' THEN '4G' "
            "WHEN cellular_coverage_type = '3G' OR cellular_coverage_type_fb = '3G' THEN '3G' "
            "WHEN cellular_coverage_type = '2G' OR cellular_coverage_type_fb = '4G' THEN '2G' "
            "ELSE 'no coverage' "
            "END"
        ),
    )
    cov_stg = cov_stg.drop("cellular_coverage_type_fb")
    cov_stg = cov_stg.withColumn(
        "cellular_coverage_availability",
        f.expr(
            "CASE " 
            "WHEN cellular_coverage_type = 'no coverage' then 'no' "
            "ELSE 'yes' "
            "END"
        ),
    )
    cov_stg = cov_stg.drop("cellular_coverage_availability_fb")

    return cov_stg


# ITU Transform Components


def itu_binary_to_boolean(df):
    df = df.withColumn("2G_coverage", f.col("2G") >= 1)
    df = df.withColumn("3G_coverage", f.col("3G") == 1)
    df = df.withColumn("4G_coverage", f.col("4G") == 1)

    df = df.drop("2G")
    df = df.drop("3G")
    df = df.drop("4G")
    return df


def itu_lower_columns(df):
    for col_name in CONFIG_ITU_COLUMNS_TO_RENAME:
        df = df.withColumnRenamed(col_name, col_name.lower())
    return df


def itu_transforms(itu):
    # fb
    itu = itu_binary_to_boolean(itu)
    itu = itu_lower_columns(itu)  ## should i remove given column mapping portal?
    itu = coverage_column_filter(itu, CONFIG_ITU_COLUMNS)
    itu = coverage_row_filter(itu)

    # coverage availability and type columns
    itu = itu.withColumn(
        "cellular_coverage_type",
        f.expr(
            "CASE "
            "WHEN 4g_coverage IS TRUE THEN '4G' "
            "WHEN 3g_coverage IS TRUE THEN '3G' "
            "WHEN 2g_coverage IS TRUE THEN '2G' "
            "ELSE 'no coverage' "
            "END"
        ),
    )
    itu = itu.withColumn(
        "cellular_coverage_availability",
        f.expr(
            "CASE " 
            "WHEN cellular_coverage_type = 'no coverage' then 'no' "
            "ELSE 'yes' "
            "END"
        ),
    )

    # add cov schema
    for col in CONFIG_COV_COLUMNS:
        if col not in itu.columns:
            itu = itu.withColumn(col, f.lit(None))

    # add suffixes
    for col in itu.columns:
        if col != "school_id_giga":  # add suffix except join key
            itu = itu.withColumnRenamed(col, col + "_itu")

    return itu


def itu_coverage_merge(itu, cov):
    # outer join
    cov_stg = cov.join(itu, on="school_id_giga", how="outer")

    # remove Xg_coverage_{source} because they are accounted for in cellular_coverage_type
    cov_stg = cov_stg.drop("2G_coverage_itu")
    cov_stg = cov_stg.drop("3G_coverage_itu")
    cov_stg = cov_stg.drop("4G_coverage_itu")

    # coalesce with updated values
    for col in cov.columns:
        if (
            col not in CONFIG_COV_COLUMN_MERGE_LOGIC and col != "school_id_giga"
        ):  # coverage availability and type are merged differently
            cov_stg = cov_stg.withColumn(col, f.coalesce(col + "_itu", col))
            cov_stg = cov_stg.drop(col + "_itu")

    # harmonize specific columns
    cov_stg = cov_stg.withColumn(
        "cellular_coverage_type",
        f.expr(
            "CASE "
            "WHEN cellular_coverage_type = '4G' OR cellular_coverage_type_itu = '4G' THEN '4G' "
            "WHEN cellular_coverage_type = '3G' OR cellular_coverage_type_itu = '3G' THEN '3G' "
            "WHEN cellular_coverage_type = '2G' OR cellular_coverage_type_itu = '4G' THEN '2G' "
            "ELSE 'no coverage' "
            "END"
        ),
    )
    cov_stg = cov_stg.drop("cellular_coverage_type_itu")
    cov_stg = cov_stg.withColumn(
        "cellular_coverage_availability",
        f.expr(
            "CASE " 
            "WHEN cellular_coverage_type = 'no coverage' then 'no' "
            "ELSE 'yes' "
            "END"
        ),
    )
    cov_stg = cov_stg.drop("cellular_coverage_availability_itu")

    return cov_stg


if __name__ == "__main__":
    from src.utils.spark import get_spark_session

    #
    file_url_fb = f"{settings.AZURE_BLOB_CONNECTION_URI}/raw/school_geolocation_coverage_data/bronze/coverage_data/UZB_school-coverage_meta_20230927-091814.csv"
    file_url_itu = f"{settings.AZURE_BLOB_CONNECTION_URI}/raw/school_geolocation_coverage_data/bronze/coverage_data/UZB_school-coverage_itu_20230927-091823.csv"
    file_url_cov = f"{settings.AZURE_BLOB_CONNECTION_URI}/raw/school_geolocation_coverage_data/silver/coverage_data/UZB_school-coverage_master.csv"
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/adls-testing-raw/_test_BLZ_RAW.csv"
    spark = get_spark_session()
    fb = spark.read.csv(file_url_fb, header=True)
    itu = spark.read.csv(file_url_itu, header=True)
    cov = spark.read.csv(file_url_cov, header=True)

    ## CONFORM TEST FILES TO PROPER SCHEMA
    fb = rename_raw_columns(fb)
    itu = rename_raw_columns(itu)
    itu = itu.withColumn("nearest_NR_id", f.lit(None))
    itu = itu.withColumn("nearest_NR_distance", f.lit(None))
    cov = rename_raw_columns(cov)
    cov = cov.withColumn("nearest_NR_id", f.lit(None))
    cov = cov.withColumn("nearest_NR_distance", f.lit(None))
    cov = cov.select(*CONFIG_COV_COLUMNS)



    ## filter to one entry for testing
    # fb = fb.filter(f.col("school_id_giga") == "a8b4968c-fcb2-31fd-83b1-01b2c48625f3")
    # itu = itu.filter(f.col("school_id_giga") == "a8b4968c-fcb2-31fd-83b1-01b2c48625f3")
    # cov = cov.filter(f.col("school_id_giga") == "a8b4968c-fcb2-31fd-83b1-01b2c48625f3")


    ## DAGSTER WORKFLOW ##

    # ## TRANSFORM STEP
    # # FB
    # fb = fb_transforms(fb)
    # df1 = fb_coverage_merge(fb, cov)  # NEED SILVER COVERAGE INPUT
    # print("Merged FB and (Silver) Coverage Dataset")
    # df1.show()

    # # ITU
    # itu = itu_transforms(itu)
    # df2 = itu_coverage_merge(itu, cov)  # NEED SILVER COVERAGE INPUT
    # print("Merged ITU and (Silver) Coverage Dataset")
    # df2.show()

    from src.spark.data_quality_tests import (row_level_checks, aggregate_report_sparkdf, aggregate_report_json)

    df = row_level_checks(itu, "coverage_itu", "UZB") # dataset plugged in should conform to updated schema! rename if necessary
    df.show()

    df = aggregate_report_sparkdf(df)
    df.show()

    _json = aggregate_report_json(df, fb)
    print(_json)
