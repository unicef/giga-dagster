import io
import re
import uuid
from itertools import chain

import geopandas as gpd
import pandas as pd
from loguru import logger
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import (
    FloatType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from azure.storage.blob import BlobServiceClient
from dagster import OpExecutionContext
from src.constants import UploadMode
from src.settings import settings
from src.spark.udf_dependencies import get_point
from src.utils.logger import get_context_with_fallback_logger

ACCOUNT_URL = "https://saunigiga.blob.core.windows.net/"
azure_sas_token = settings.AZURE_SAS_TOKEN
azure_blob_container_name = settings.AZURE_BLOB_CONTAINER_NAME
container_name = azure_blob_container_name


# STANDARDIZATION FUNCTIONS
def generate_uuid(identifier_concat: str) -> str:
    return str(uuid.uuid3(uuid.NAMESPACE_DNS, str(identifier_concat)))


generate_uuid_udf = f.udf(generate_uuid)


def create_school_id_giga(df: sql.DataFrame) -> sql.DataFrame:
    # Create school_id_giga column if not exists, otherwise use provided values
    df = df.withColumn(
        "school_id_giga", f.coalesce(f.col("school_id_giga"), f.lit(None))
    )

    school_id_giga_prereqs = [
        "school_id_govt",
        "school_name",
        "education_level",
        "latitude",
        "longitude",
    ]
    for column in school_id_giga_prereqs:
        if column not in df.columns:
            return df.withColumn("school_id_giga", f.lit(None))

    df = df.withColumn(
        "identifier_concat",
        f.concat(
            f.col("school_id_govt").cast(StringType()),
            f.col("school_name").cast(StringType()),
            f.col("education_level").cast(StringType()),
            f.col("latitude").cast(StringType()),
            f.col("longitude").cast(StringType()),
        ),
    )

    # Use existing school_id_giga if provided, otherwise use system-generated value
    df = df.withColumn(
        "school_id_giga",
        f.coalesce(
            f.col("school_id_giga"),
            f.when(
                (f.col("school_id_govt").isNull())
                | (f.col("school_name").isNull())
                | (f.col("education_level").isNull())
                | (f.col("latitude").isNull())
                | (f.col("longitude").isNull()),
                f.lit(None),
            ).otherwise(generate_uuid_udf(f.col("identifier_concat"))),
        ),
    )

    return df.drop("identifier_concat")


def create_education_level(df: sql.DataFrame) -> sql.DataFrame:
    education_level_govt_mapping = {
        # None : "Unknown",
        "Other": "Unknown",
        "Unknown": "Unknown",
        "Pre-Primary And Primary And Secondary": "Pre-Primary, Primary and Secondary",
        "Primary, Secondary And Post-Secondary": "Primary, Secondary and Post-Secondary",
        "Pre-Primary, Primary, And Secondary": "Pre-Primary, Primary and Secondary",
        "Basic": "Primary",
        "Basic And Secondary": "Primary and Secondary",
        "Pre-Primary": "Pre-Primary",
        "Primary": "Primary",
        "Secondary": "Secondary",
        "Post-Secondary": "Post-Secondary",
        "Pre-Primary And Primary": "Pre-Primary and Primary",
        "Primary And Secondary": "Primary and Secondary",
        "Pre-Primary, Primary And Secondary": "Pre-Primary, Primary and Secondary",
    }

    mapped_column = f.create_map(
        [f.lit(x) for x in chain(*education_level_govt_mapping.items())]
    )

    if "education_level" in df.columns:
        df = df.withColumn(
            "mapped_column", mapped_column[f.col("education_level_govt")]
        )
        df = df.withColumn(
            "education_level",
            f.coalesce(f.col("education_level"), f.col("mapped_column")),
        ).drop("mapped_column")
    else:
        df = df.withColumn(
            "education_level", mapped_column[f.col("education_level_govt")]
        )

    # fill nulls with "Unknown"
    return df.withColumns(
        {
            "education_level_govt": f.coalesce(
                f.col("education_level_govt"), f.lit("Unknown")
            ),
            "education_level": f.coalesce(f.col("education_level"), f.lit("Unknown")),
        }
    )


def create_uzbekistan_school_name(df: sql.DataFrame) -> sql.DataFrame:
    school_name_col = "school_name"
    district_col = "district"
    city_col = "city"
    region_col = "region"

    # spark doesnt have a function like pd.notna, checking first for column existence
    if school_name_col not in df.columns:
        df = df.withColumn(school_name_col, f.lit(None).cast("string"))
    elif district_col not in df.columns:
        df = df.withColumn(district_col, f.lit(None).cast("string"))
    elif city_col not in df.columns:
        df = df.withColumn(city_col, f.lit(None).cast("string"))
    elif region_col not in df.columns:
        df = df.withColumn(region_col, f.lit(None).cast("string"))

    # case when expr for concats
    return df.withColumn(
        "school_name",
        f.expr(
            "CASE "
            "WHEN district IS NOT NULL AND region IS NOT NULL THEN "
            "CONCAT(school_name, ',', district, ',', region) "
            "WHEN district IS NOT NULL AND city IS NOT NULL THEN "
            "CONCAT(school_name, ',', city, ',', district) "
            "WHEN city IS NOT NULL AND region IS NOT NULL THEN "
            "CONCAT(school_name, ',', city, ',', region) "
            " ELSE CONCAT(COALESCE(school_name, ''), ',', COALESCE(region, ''), ',', COALESCE(region, '')) END",
        ),
    )


def standardize_school_name(df: sql.DataFrame) -> sql.DataFrame:
    # filter
    df1 = df.filter(df.country_code == "UZB")
    df2 = df.filter(df.country_code != "UZB")

    # uzb transform
    df1 = create_uzbekistan_school_name(df1)
    return df2.union(df1)


def standardize_internet_speed(df: sql.DataFrame) -> sql.DataFrame:
    return df.withColumn(
        "download_speed_govt",
        f.regexp_replace(f.col("download_speed_govt"), "[^0-9.]", "").cast(FloatType()),
    )


def standardize_connectivity_type(df: sql.DataFrame) -> sql.DataFrame:
    type_conn_regex_patterns = {
        "fibre": "fiber|fibre|fibra|ftt|fttx|ftth|fttp|gpon|epon|fo|Фибер|optic|птички",
        "copper": "adsl|dsl|copper|hdsl|vdsl",
        "coaxial": "coax|coaxial",
        "wired_other": "wired|ethernet|kablovski",
        "unknown_wired": "unknown_wired",
        "cellular": "cell|cellular|celular|2g|3g|4g|5g|lte|gsm|umts|cdma|mobile|mobie|p2a",
        "p2p": "p2p|radio|microwave|ptmp|micro.wave|wimax|optical",
        "satellite": "satellite|satelite|vsat|geo|leo|meo",
        "haps": "haps",
        "drones": "drones",
        "unknown_wireless": "unknown_wireless",
        "other": "TVWS|other|ethernet",
        "unknown": "unknown|null|nan|n/a",
    }

    connectivity_root_mappings = {
        "wired": ["fibre", "copper", "coaxial", "wired_other", "unknown_wired"],
        "wireless": [
            "cellular",
            "p2p",
            "satellite",
            "haps",
            "drones",
            "unknown_wireless",
            "other_wireless",
        ],
        "unknown_connectivity_type": ["unknown"],
    }

    def clean_type_connectivity(value):
        for cleaned, matches in type_conn_regex_patterns.items():
            if pd.isna(value):
                return "unknown"
            elif re.search(matches, str(value).lower(), flags=re.I):
                return cleaned
        return "unknown"

    clean_type_connectivity_udf = f.udf(clean_type_connectivity, StringType())

    def get_connectivity_type_root(value):
        for key in connectivity_root_mappings:
            if value in connectivity_root_mappings[key]:
                return key

    get_connectivity_type_root_udf = f.udf(get_connectivity_type_root, StringType())

    df = df.withColumn(
        "connectivity_type", clean_type_connectivity_udf(df["connectivity_type_govt"])
    )
    df = df.withColumn(
        "connectivity_type_root",
        get_connectivity_type_root_udf(df["connectivity_type"]),
    )

    return df


def column_mapping_rename(
    df: sql.DataFrame,
    column_mapping: dict[str, str],
) -> tuple[sql.DataFrame, dict[str, str]]:
    column_mapping_filtered = {
        k: v for k, v in column_mapping.items() if (k is not None) and (v is not None)
    }
    return df.withColumnsRenamed(column_mapping_filtered), column_mapping_filtered


def add_missing_columns(
    df: sql.DataFrame, schema_columns: list[StructField]
) -> sql.DataFrame:
    columns_to_add = {
        col.name: f.lit(None).cast(col.dataType)
        for col in schema_columns
        if col.name not in df.columns
    }
    return df.withColumns(columns_to_add)


def add_missing_values(
    df: sql.DataFrame, schema_columns: list[StructField]
) -> sql.DataFrame:
    columns_to_fill = [col.name for col in schema_columns]
    column_actions = {}

    column_actions = {
        c: f.coalesce(f.col(c), f.lit("Unknown")) for c in columns_to_fill
    }

    df = df.withColumns(column_actions)
    return df


def bronze_prereq_columns(df, schema_columns: list[StructField]) -> sql.DataFrame:
    column_names = [col.name for col in schema_columns]
    return df.select(*column_names)


# Note: Temporary function for transforming raw files to standardized columns.
def create_bronze_layer_columns(
    df: sql.DataFrame,
    silver: sql.DataFrame,
    country_code_iso3: str,
    is_qos: bool = False,
) -> sql.DataFrame:
    """Create bronze layer columns with optional QoS-specific handling.

    Args:
        df: Input DataFrame
        silver: Reference silver DataFrame
        country_code_iso3: Country code in ISO3 format
        is_qos: Whether to apply QoS-specific transformations

    Returns:
        DataFrame with bronze layer columns added
    """
    # Handle timestamp columns for QoS data
    if is_qos:
        df = df.withColumns(
            {
                col.name: f.col(col.name).cast(TimestampType())
                for col in df.schema
                if col.name.endswith("_timestamp")
            }
        )

    # Join with silver data
    joined_df = df.alias("df").join(
        silver.alias("silver"), on="school_id_govt", how="left"
    )

    # Get column lists
    columns_in_silver_only = [col for col in silver.columns if col not in df.columns]
    common_columns = [col for col in df.columns if col in silver.columns]

    # Build select expression
    select_expr = [
        f.coalesce(f.col(f"df.{col}"), f.col(f"silver.{col}")).alias(col)
        for col in common_columns
    ]
    select_expr.extend(
        [f.col(f"silver.{col}").alias(col) for col in columns_in_silver_only]
    )

    # Select columns from joined DataFrame
    df = joined_df.select(*select_expr)

    # standardize education level
    df = create_education_level(df)
    df = create_school_id_giga(df)
    df = df.withColumn(
        "school_id_govt_type",
        f.coalesce(f.col("school_id_govt_type"), f.lit("Unknown")),
    )

    # Admin mapbox columns
    df = add_admin_columns(
        df=df,
        country_code_iso3=country_code_iso3,
        admin_level="admin1",
    )
    df = add_admin_columns(
        df=df,
        country_code_iso3=country_code_iso3,
        admin_level="admin2",
    )
    df = add_disputed_region_column(df=df)

    # Add ingestion timestamp
    return df.withColumn(
        "connectivity_govt_ingestion_timestamp",
        f.when(
            f.col("connectivity_govt_ingestion_timestamp").isNull(),
            f.current_timestamp(),
        ),
    )


def get_admin_boundaries(
    country_code_iso3: str,
    admin_level: str,
) -> pd.DataFrame | gpd.GeoDataFrame | None:  # admin level = ["admin1", "admin2"]
    try:
        service = BlobServiceClient(account_url=ACCOUNT_URL, credential=azure_sas_token)
        filename = f"{country_code_iso3}_{admin_level}.geojson"
        file = f"admin_data/{admin_level}/{filename}"
        blob_client = service.get_blob_client(container=container_name, blob=file)
        with io.BytesIO() as file_blob:
            download_stream = blob_client.download_blob()
            download_stream.readinto(file_blob)
            file_blob.seek(0)
            return gpd.read_file(file_blob)
    except Exception as exc:
        logger.error(exc)
        return None


def add_admin_columns(  # noqa: C901
    df: sql.DataFrame,
    country_code_iso3: str,
    admin_level: str,
) -> sql.DataFrame:
    admin_boundaries = get_admin_boundaries(
        country_code_iso3=country_code_iso3,
        admin_level=admin_level,
    )

    if admin_boundaries is None:
        return df.withColumns(
            {
                admin_level: f.coalesce(f.col(admin_level), f.lit("Unknown")),
                f"{admin_level}_id_giga": f.coalesce(
                    f.col(f"{admin_level}_id_giga"), f.lit(None)
                ),
            }
        )

    spark = df.sparkSession
    broadcasted_admin_boundaries = spark.sparkContext.broadcast(admin_boundaries)

    def get_admin_en(latitude, longitude) -> str | None:
        point = get_point(longitude=longitude, latitude=latitude)
        for _, row in broadcasted_admin_boundaries.value.iterrows():
            if row.geometry.contains(point):
                return row.get("name_en")
        return None

    get_admin_en_udf = f.udf(get_admin_en, StringType())

    def get_admin_native(latitude, longitude) -> str | None:
        point = get_point(longitude=longitude, latitude=latitude)
        for _, row in broadcasted_admin_boundaries.value.iterrows():
            if row.geometry.contains(point):
                return row.get("name")
        return None

    get_admin_native_udf = f.udf(get_admin_native, StringType())

    def get_admin_id_giga(latitude, longitude) -> str | None:
        point = get_point(longitude=longitude, latitude=latitude)
        for _, row in broadcasted_admin_boundaries.value.iterrows():
            if row.geometry.contains(point):
                return row.get(f"{admin_level}_id_giga")
        return None

    get_admin_id_giga_udf = f.udf(get_admin_id_giga, StringType())

    df = df.withColumns(
        {
            f"{admin_level}_en": get_admin_en_udf(df["latitude"], df["longitude"]),
            f"{admin_level}_native": get_admin_native_udf(
                df["latitude"], df["longitude"]
            ),
            f"{admin_level}_id_giga": get_admin_id_giga_udf(
                df["latitude"], df["longitude"]
            ),
        }
    )
    return df.withColumn(
        admin_level,
        f.coalesce(
            f.col(f"{admin_level}_en"),
            f.col(f"{admin_level}_native"),
            f.col(admin_level),
            f.lit("Unknown"),
        ),
    ).drop(f"{admin_level}_en", f"{admin_level}_native")


def add_disputed_region_column(df: sql.DataFrame) -> sql.DataFrame:
    try:
        service = BlobServiceClient(account_url=ACCOUNT_URL, credential=azure_sas_token)
        filename = "disputed_areas_admin0.geojson"
        file = f"admin_data/admin0/{filename}"
        blob_client = service.get_blob_client(container=container_name, blob=file)
        with io.BytesIO() as file_blob:
            download_stream = blob_client.download_blob()
            download_stream.readinto(file_blob)
            file_blob.seek(0)
            admin_boundaries = gpd.read_file(file_blob)

    except Exception as exc:
        logger.error(exc)
        admin_boundaries = None

    spark = df.sparkSession
    broadcasted_admin_boundaries = spark.sparkContext.broadcast(admin_boundaries)

    def get_disputed_region(latitude, longitude) -> str | None:
        point = get_point(longitude=longitude, latitude=latitude)
        for _, row in broadcasted_admin_boundaries.value.iterrows():
            if row.geometry.contains(point):
                return row.get("name")
        return None

    get_disputed_region_udf = f.udf(get_disputed_region, StringType())

    if admin_boundaries is None:
        df = df.withColumn("disputed_region", f.lit(None))
    else:
        df = df.withColumn(
            "disputed_region",
            get_disputed_region_udf(df["latitude"], df["longitude"]),
        )

    return df


def connectivity_rt_dataset(
    spark: SparkSession,
    iso2_country_code: str,
    is_test=True,
    context: OpExecutionContext = None,
):
    from src.internal.connectivity_queries import (
        get_giga_meter_schools,
        get_mlab_schools,
        get_rt_schools,
    )

    logger = get_context_with_fallback_logger(context)

    # get raw datasets
    rt_data = get_rt_schools(iso2_country_code, is_test=is_test)
    mlab_data = get_mlab_schools(iso2_country_code, is_test=is_test)
    dca_data = get_giga_meter_schools(is_test=is_test)

    # Assert schemas
    all_rt_schema = StructType(
        [
            StructField("connectivity_rt_ingestion_timestamp", TimestampType(), True),
            StructField("country", StringType(), True),
            StructField("country_code", StringType(), True),
            StructField("school_id_giga", StringType(), True),
            StructField("school_id_govt", StringType(), True),
        ]
    )
    # Create the DataFrame
    if rt_data.empty:
        rt_data = []

    df_all_rt = spark.createDataFrame(rt_data, all_rt_schema)

    mlab_schema = StructType(
        [
            StructField("country_code", StringType(), True),
            StructField("mlab_created_date", StringType(), True),
            StructField("school_id_govt", StringType(), True),
            StructField("source", StringType(), True),
        ]
    )
    # Create the DataFrame
    if mlab_data.empty:
        mlab_data = []

    df_mlab = spark.createDataFrame(mlab_data, mlab_schema)

    dca_schema = StructType(
        [
            StructField("school_id_giga", StringType(), True),
            StructField("school_id_govt", StringType(), True),
            StructField("source", StringType(), True),
        ]
    )

    if dca_data.empty:
        dca_data = []

    df_dca = spark.createDataFrame(dca_data, dca_schema)

    # transforms
    # cast to proper format
    # df_all_rt = df_all_rt.withColumn(
    #     "connectivity_rt_ingestion_timestamp",
    #     f.to_timestamp(
    #         f.col("connectivity_rt_ingestion_timestamp"),
    #         "yyyy-MM-dd HH:mm:ss.SSSSSSXXX",
    #     ),
    # )
    # df_mlab = df_mlab.withColumn(
    #     "mlab_created_date",
    #     f.to_date(f.col("mlab_created_date"), "yyyy-MM-dd").cast(StringType()),
    # )

    # dataset prefixes
    column_renames = {col: f"{col}_mlab" for col in df_mlab.schema.fieldNames()}
    df_mlab = df_mlab.withColumnsRenamed(column_renames)
    # df_mlab.show()

    column_renames = {col: f"{col}_pcdc" for col in df_dca.schema.fieldNames()}
    df_dca = df_dca.withColumnsRenamed(column_renames)
    # df_dca.show()
    # df_all_rt.show()

    # merge three datasets
    all_rt_schools = df_all_rt.join(
        df_mlab,
        how="left",
        on=[
            df_all_rt.school_id_govt == df_mlab.school_id_govt_mlab,
            df_all_rt.country_code == df_mlab.country_code_mlab,
        ],
    ).join(
        df_dca, how="left", on=df_all_rt.school_id_giga == df_dca.school_id_giga_pcdc
    )

    # create source column
    all_rt_schools = all_rt_schools.withColumn(
        "source",
        f.regexp_replace(
            f.concat_ws(
                ", ", f.trim(f.col("source_pcdc")), f.trim(f.col("source_mlab"))
            ),
            "^, |, $",
            "",
        ),
    )

    all_rt_schools = all_rt_schools.withColumn(
        "connectivity_RT_datasource",
        f.when(
            (f.col("source") == "") & (f.col("country") == "Brazil"), "nic_br"
        ).otherwise(f.col("source")),
    )

    # select relevant columns
    realtime_columns = [
        "school_id_giga",
        # "country",
        # "school_id_govt",
        "connectivity_RT_ingestion_timestamp",
        "connectivity_RT_datasource",
        "connectivity_RT",
    ]
    all_rt_schools = all_rt_schools.withColumn("connectivity_RT", f.lit("Yes"))
    all_rt_schools = all_rt_schools.filter(f.col("connectivity_RT_datasource") != "")  #

    out = all_rt_schools.select(*realtime_columns)
    logger.info(out.schema)
    return out


def merge_connectivity_to_master(
    master: sql.DataFrame,
    connectivity: sql.DataFrame,
    uploaded_columns: list,
    mode: str,
):
    connectivity_columns = [
        col for col in connectivity.columns if col != "school_id_giga"
    ]
    columns_to_drop = [col for col in connectivity_columns if col in master.columns]

    master = master.drop(*columns_to_drop)

    master = master.join(connectivity, on="school_id_giga", how="left")

    master = master.withColumn(
        "connectivity_govt",
        f.when(
            f.isnan(f.col("connectivity_govt")), f.lit(None).cast(StringType())
        ).otherwise(f.col("connectivity_govt")),
    )

    if mode == UploadMode.CREATE.value or {
        "download_speed_govt",
        "connectivity_govt",
    }.issubset(set(uploaded_columns)):
        # this block will run when we create schools or during updates if both download_speed_govt
        # and connectivity_govt re provided

        master = master.withColumn(
            "connectivity",
            f.when(
                (f.lower(f.col("connectivity_RT")) == "yes")
                | (
                    (f.lower(f.col("connectivity_govt")) == "yes")
                    & (
                        (f.col("download_speed_govt") != 0)
                        | f.col("download_speed_govt").isNull()
                    )
                )
                | (f.col("download_speed_govt") > 0),
                "Yes",
            )
            .when(
                (f.lower(f.col("connectivity_govt")).isNull()),
                f.lit(None) if mode == UploadMode.UPDATE.value else f.lit("Unknown"),
            )
            .otherwise("No"),
        )
    elif "connectivity_govt" in uploaded_columns:
        # this will run during updates if only connectivity_govt is uploaded
        master = master.withColumn(
            "connectivity",
            f.when(f.lower(f.col("connectivity_govt")) == "yes", "Yes")
            .when(f.lower(f.col("connectivity_govt")) == "no", "No")
            .otherwise(f.lit(None).cast(StringType())),
        )
    elif "download_speed_govt" in uploaded_columns:
        # this will run during updates if only download_speed_govt is uploaded
        master = master.withColumn(
            "connectivity",
            f.when(f.col("download_speed_govt") > 0, "Yes")
            .when(f.col("download_speed_govt") == 0, "No")
            .otherwise(f.lit(None).cast(StringType())),
        )

    # sanitize connectivity_govt if provided
    if "connectivity_govt" in master.columns:
        master = master.withColumn(
            "connectivity_govt", f.initcap(f.trim(f.col("connectivity_govt")))
        )

    master.withColumn(
        "connectivity_RT", f.coalesce(f.col("connectivity_RT"), f.lit("No"))
    )

    return master


if __name__ == "__main__":
    from src.utils.spark import get_spark_session

    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/bronze/school-geolocation-data/BLZ_school-geolocation_gov_20230207.csv"
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/bronze/school-geolocation/SEN/wwx7232jufo9htsuq595zy07_SEN_geolocation_20240610-163027.csv"
    file_url_master = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/master/BRA_school_geolocation_coverage_master.csv"
    file_url_reference = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/reference/BRA_master_reference.csv"
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/adls-testing-raw/_test_BLZ_RAW.csv"

    spark = get_spark_session()
    master = spark.read.csv(file_url_master, header=True)
    reference = spark.read.csv(file_url_reference, header=True)
    # geolocation = spark.read.csv(file_url, header=True)
    # df_bronze = spark.read.csv(file_url, header=True)
    gold = master.join(reference, how="left", on="school_id_giga")
    geolocation_columns = [
        "school_id_giga",
        "school_id_govt",
        "school_name",
        "school_establishment_year",
        "latitude",
        "longitude",
        "education_level",
        "education_level_govt",
        "connectivity_govt",
        "connectivity_govt_ingestion_timestamp",
        "connectivity_govt_collection_year",
        "download_speed_govt1",
        "download_speed_contracted",
        "connectivity_type_govt",
        "admin1",
        "admin1_id_giga",
        "admin2",
        "admin2_id_giga",
        "disputed_region",
        "school_area_type",
        "school_funding_type",
        "num_computers",
        "num_computers_desired",
        "num_teachers",
        "num_adm_personnel",
        "num_students",
        "num_classrooms",
        "num_latrines",
        "computer_lab",
        "electricity_availability",
        "electricity_type",
        "water_availability",
        "school_data_source",
        "school_data_collection_year",
        "school_data_collection_modality",
        "school_id_govt_type",
        "school_address",
        "is_school_open",
        "school_location_ingestion_timestamp",
    ]
    silver = gold.select(*geolocation_columns)
    silver = silver.withColumnRenamed("download_speed_govt1", "download_speed_govt")
    silver.show()

    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    # Initialize Spark session
    spark = SparkSession.builder.appName("CreateDataFrame").getOrCreate()

    # Define the schema
    schema = StructType(
        [
            StructField("school_id_govt", StringType(), True),
            StructField("school_name", StringType(), True),
            StructField("education_level", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
        ]
    )

    # Create the data
    data = [
        # ("11000023", "EEEE ABNAEL MACHADO DE LIMA - CENE", "Unknown", -8.758459, -63.85401),
        ("11000023", None, None, 69.1, None),  # update
        # ("11000040", "EMEIEF PEQUENOS TALENTOS", "Unknown", -8.79373, -63.88392)
        ("11000041", "test", "Unknown", -8.79373, -63.88392),  # new
    ]

    # Create DataFrame
    df = spark.createDataFrame(data, schema)

    # Show DataFrame
    df.show()

    df = create_bronze_layer_columns(df=df, silver=silver, country_code_iso3="BRA")
    df.show()

    from src.data_quality_checks.utils import (
        aggregate_report_spark_df,
        row_level_checks,
    )

    # df = update_checks(bronze=df, silver=silver)
    # df = create_checks(bronze=df, silver=silver)
    # df.show()

    df = row_level_checks(
        df=df,
        silver=silver,
        mode=UploadMode.UPDATE.value,
        dataset_type="geolocation",
        _country_code_iso3="BRA",
    )
    df.show()

    df = aggregate_report_spark_df(spark=df.sparkSession, df=df)
    df.show(500)
