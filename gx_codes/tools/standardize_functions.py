import uuid

import pandas as pd


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


def create_uzbekistan_school_name(row):
    city = row["city"]
    district = row["district"]
    region = row["region"]
    school_name = row["school_name"]

    if pd.notna(district) and pd.notna(region):
        return f"{school_name},{district},{region}"
    elif pd.notna(district) and pd.notna(city):
        return f"{school_name},{city},{district}"
    elif pd.notna(city) and pd.notna(region):
        return f"{school_name},{city},{region}"
    else:
        return f"{school_name},{region},{region}"


# df['hex8'] = df.apply(lambda row: h3.geo_to_h3(float(row['latitude']), float(row['longitude']), resolution=8),
#                         axis='columns')
# df['school_density'] = df.groupby(['hex8'])['giga_id_school'].transform('size')
