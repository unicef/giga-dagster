import uuid

import h3
import pandas as pd
from check_functions import get_decimal_places, has_no_similar_name
from config_expectations import CONFIG_VALUES_RANGE


# STANDARDIZATION FUNCTIONS
def create_giga_school_id(row):
    school_id = str(row["school_id"])
    school_name = str(row["school_name"])
    education_level = str(row["education_level"])
    latitude = str(row["latitude"])
    longitude = str(row["longitude"])

    return str(
        uuid.uuid3(
            uuid.NAMESPACE_DNS,
            (
                str(school_id)
                + str(school_name)
                + str(education_level)
                + str(latitude)
                + str(longitude)
            ),
        )
    )


def create_uzbekistan_school_name(city, district, region, school_name):
    if pd.notna(district) and pd.notna(region):
        return f"{school_name},{district},{region}"
    elif pd.notna(district) and pd.notna(city):
        return f"{school_name},{city},{district}"
    elif pd.notna(city) and pd.notna(region):
        return f"{school_name},{city},{region}"
    else:
        return f"{school_name},{region},{region}"


def standardize_school_name(row):
    country_code = row["country_code"]

    if country_code == "UZB":
        city = row["city"]
        district = row["district"]
        region = row["region"]
        school_name = row["school_name"]
        return create_uzbekistan_school_name(city, district, region, school_name)

    return school_name


# Note: Temporary function for transforming raw files to standardized columns.
# This should eventually be converted to dbt transformations.
def create_bronze_layer_columns(df):
    # ID
    df["giga_id_school"] = df.apply(create_giga_school_id, axis="columns")
    # School Density Computation
    df["hex8"] = df.apply(
        lambda row: h3.geo_to_h3(
            float(row["latitude"]), float(row["longitude"]), resolution=8
        ),
        axis="columns",
    )
    df["school_density"] = df.groupby(["hex8"])["giga_id_school"].transform("size")
    # Special Cases
    df["school_name"] = df.apply(standardize_school_name, axis="columns")

    return df


def is_within_range(value, min, max):
    return value >= min and value <= max


def create_staging_layer_columns(df):
    df["precision_longitude"] = df["longitude"].apply(get_decimal_places)
    df["precision_latitude"] = df["latitude"].apply(get_decimal_places)
    df["missing_school_name_flag"] = df["school_name"].apply(
        lambda x: pd.isna(x), axis=1
    )
    df["missing_internet_availability_flag"] = df["internet_availability"].apply(
        lambda x: pd.isna(x), axis=1
    )
    df["missing_internet_speed_Mbps_flag"] = df["internet_availability"].apply(
        lambda x: pd.isna(x), axis=1
    )
    df["missing_internet_type_flag"] = df["internet_type"].apply(
        lambda x: pd.isna(x), axis=1
    )
    df["internet_speed_outlier_flag"] = df["internet_speed"].apply(
        lambda x: is_within_range(
            x,
            CONFIG_VALUES_RANGE["internet_speed_mbps"]["min"],
            CONFIG_VALUES_RANGE["internet_speed_mbps"]["min"],
        ),
        axis=1,
    )
    # df["school_type"] =
    df["school_density_outlier_flag"] = df["school_density"].apply(
        lambda x: is_within_range(
            x,
            CONFIG_VALUES_RANGE["school_density"]["min"],
            CONFIG_VALUES_RANGE["school_density"]["max"],
        ),
        axis=1,
    )

    df["duplicate_school_id"] = df.duplicated(subset=["school_id"], keep=False)
    df["duplicate_id_name_level_location"] = df.duplicated(
        subset=["school_id", "school_name", "education_level", "latitude", "longitude"],
        keep=False,
    )
    df["duplicate_name_level_location"] = df.duplicated(
        subset=["school_name", "education_level", "latitude", "longitude"], keep=False
    )
    df["duplicate_level_location"] = df.duplicated(
        subset=["education_level", "latitude", "longitude"], keep=False
    )
    df["has_similar_name"] = not has_no_similar_name(df["school_name"])
    # df["duplicate_same_name_level_within_110m_radius"] =
    # df["duplicate_similar_name_same_level_within_100m_radius"] =
    # df["duplicate"] =
    # df["duplicate_id"] =
    # df["duplicate_all_except_school_id"] =
