import uuid

import h3
import pandas as pd


# STANDARDIZATION FUNCTIONS
def create_giga_school_id(school_id, school_name, education_level, latitude, longitude):
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

    return df


def create_uzbekistan_school_name(city, district, region, school_name):
    if pd.notna(district) and pd.notna(region):
        return f"{school_name},{district},{region}"
    elif pd.notna(district) and pd.notna(city):
        return f"{school_name},{city},{district}"
    elif pd.notna(city) and pd.notna(region):
        return f"{school_name},{city},{region}"
    else:
        return f"{school_name},{region},{region}"
