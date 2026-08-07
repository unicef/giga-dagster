import io
import uuid
from itertools import chain

import country_converter as coco
import geopandas as gpd
import pandas as pd
from delta import DeltaTable
from loguru import logger
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import (
    FloatType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dagster import OpExecutionContext
from src.internal.connectivity_queries import get_qos_tables
from src.settings import DeploymentEnvironment, settings
from src.spark.udf_dependencies import get_point
from src.utils.adls import get_blob_service_client
from src.utils.logger import get_context_with_fallback_logger
from src.utils.nocodb.get_nocodb_data import (
    get_nocodb_table_as_key_value_mapping,
    get_nocodb_table_rows,
)
from src.utils.schema import construct_full_table_name
from src.utils.school_registrations.common import (
    set_school_registration_verification_status,
)

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
    # df = df.withColumn(
    #     "school_id_giga", f.coalesce(f.col("school_id_giga"), f.lit(None))
    # )

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

    # If school_id_giga was not part of the upload, add it as null so the coalesce
    # below can reference it safely regardless of whether the column already exists.
    if "school_id_giga" not in df.columns:
        df = df.withColumn("school_id_giga", f.lit(None).cast(StringType()))

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


def create_health_id_giga(df: sql.DataFrame) -> sql.DataFrame:
    # Create health_id_giga column if not exists, otherwise use provided values
    df = df.withColumn(
        "health_id_giga", f.coalesce(f.col("health_id_giga"), f.lit(None))
    )

    health_id_giga_prereqs = [
        "facility_name",
        "latitude",
        "longitude",
    ]
    for column in health_id_giga_prereqs:
        if column not in df.columns:
            return df.withColumn("health_id_giga", f.lit(None))

    df = df.withColumn(
        "identifier_concat",
        f.concat(
            f.col("facility_name").cast(StringType()),
            f.col("latitude").cast(StringType()),
            f.col("longitude").cast(StringType()),
        ),
    )

    # Use existing health_id_giga if provided, otherwise use system-generated value
    df = df.withColumn(
        "health_id_giga",
        f.coalesce(
            f.col("health_id_giga"),
            f.when(
                (f.col("facility_name").isNull())
                | (f.col("latitude").isNull())
                | (f.col("longitude").isNull()),
                f.lit(None),
            ).otherwise(generate_uuid_udf(f.col("identifier_concat"))),
        ),
    )

    return df.drop("identifier_concat")


def map_govt_to_giga_columns(
    df: sql.DataFrame, uploaded_columns: list[str]
) -> sql.DataFrame:
    rows = get_nocodb_table_rows(
        settings.NOCODB_NAME_MAPPINGS_TABLE_ID,
        where="(column_name,notblank)",
        fields="column_name,target_column,table_id",
    )

    for row in rows:
        source_col = row.get("column_name", "")
        target_col = row.get("target_column", "")
        table_id = row.get("table_id", "")

        if not (source_col and target_col and table_id):
            continue
        if source_col not in uploaded_columns:
            continue

        mapping = get_nocodb_table_as_key_value_mapping(table_id=table_id)
        if not mapping:
            df = df.withColumn(target_col, f.lit("Unknown"))
            continue

        govt_casing_map = f.create_map(
            [
                f.lit(x)
                for x in chain(*{k.lower().strip(): k.strip() for k in mapping}.items())
            ]
        )
        govt_to_giga_map = f.create_map(
            [
                f.lit(x)
                for x in chain(
                    *{k.lower().strip(): v.strip() for k, v in mapping.items()}.items()
                )
            ]
        )
        df = df.withColumn(
            source_col,
            f.coalesce(
                govt_casing_map[f.lower(f.trim(f.col(source_col)))], f.col(source_col)
            ),
        )
        df = df.withColumn(
            target_col,
            f.coalesce(
                govt_to_giga_map[f.lower(f.trim(f.col(source_col)))], f.lit("Unknown")
            ),
        )

    return df


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


def get_connectivity_type_root(value):
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
        "other": ["other"],
        "unknown_connectivity_type": ["unknown"],
    }
    for key in connectivity_root_mappings:
        if value in connectivity_root_mappings[key]:
            return key


def column_mapping_rename(
    df: sql.DataFrame,
    column_mapping: dict[str, str],
) -> tuple[sql.DataFrame, dict[str, str]]:
    column_mapping_filtered = {
        k.strip(): v
        for k, v in column_mapping.items()
        if (k is not None) and (v is not None)
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
    uploaded_columns: list[str],
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

    # Generate school_id_giga for new schools using the dedicated function
    df_with_giga = create_school_id_giga(df)
    df = df.withColumn(
        "school_id_giga",
        f.when(f.col("is_new_school"), df_with_giga["school_id_giga"]).otherwise(
            f.col("school_id_giga")
            if "school_id_giga" in df.columns
            else f.lit(None).cast(StringType())
        ),
    )

    if "school_id_govt_type" in uploaded_columns:
        df = df.withColumn(
            "school_id_govt_type",
            f.coalesce(f.col("school_id_govt_type"), f.lit(None).cast(StringType())),
        )
    df = df.withColumn(
        "school_id_govt_type",
        f.when(
            f.col("is_new_school") & f.col("school_id_govt_type").isNull(),
            f.lit("Unknown"),
        ).otherwise(f.col("school_id_govt_type")),
    )

    # Admin mapbox columns
    if "latitude" in uploaded_columns and "longitude" in uploaded_columns:
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
        df = add_admin_columns(
            df=df,
            country_code_iso3=country_code_iso3,
            admin_level="admin3",
        )
        df = add_admin_columns(
            df=df,
            country_code_iso3=country_code_iso3,
            admin_level="admin4",
        )
        df = add_disputed_region_column(df=df)

        missing_location_condition = (
            f.col("latitude").isNull()
            | f.isnan(f.col("latitude"))
            | f.col("longitude").isNull()
            | f.isnan(f.col("longitude"))
        )
        for column in ("admin1", "admin1_id_giga", "admin2", "admin2_id_giga"):
            df = df.withColumn(
                column,
                f.when(
                    missing_location_condition, f.lit(None).cast(StringType())
                ).otherwise(f.col(column)),
            )

    return df


def create_bronze_layer_columns_updated(
    df: sql.DataFrame,
    uploaded_columns: list[str],
    country_code_iso3: str,
    source: str = None,
    spark: SparkSession = None,
):
    df = map_govt_to_giga_columns(df, uploaded_columns)

    if "connectivity_type_govt" in uploaded_columns:
        df = df.withColumn("connectivity_type", f.lower(f.col("connectivity_type")))
        get_connectivity_type_root_udf = f.udf(get_connectivity_type_root, StringType())
        df = df.withColumn(
            "connectivity_type_root",
            f.when(
                f.col("connectivity_type_govt").isNotNull(),
                get_connectivity_type_root_udf(f.col("connectivity_type")),
            ).otherwise(f.lit(None).cast(StringType())),
        )

    # Generate school_id_giga for new schools only; existing schools get null here
    # and are enriched from silver later in enrich_with_silver_values.
    df = create_school_id_giga(df).withColumn(
        "school_id_giga",
        f.when(f.col("is_new_school"), f.col("school_id_giga")).otherwise(
            f.lit(None).cast(StringType())
        ),
    )

    if "school_id_govt_type" not in df.columns:
        df = df.withColumn("school_id_govt_type", f.lit(None).cast(StringType()))
    df = df.withColumn(
        "school_id_govt_type",
        f.when(
            f.col("is_new_school") & f.col("school_id_govt_type").isNull(),
            f.lit("Unknown"),
        ).otherwise(f.col("school_id_govt_type")),
    )

    # Admin columns: re-compute whenever lat/lon are part of the upload
    if "latitude" in uploaded_columns and "longitude" in uploaded_columns:
        for admin_level in ("admin1", "admin2", "admin3", "admin4"):
            df = add_admin_columns(df, country_code_iso3, admin_level)
        df = add_disputed_region_column(df)

        missing_location_condition = (
            f.col("latitude").isNull()
            | f.isnan(f.col("latitude"))
            | f.col("longitude").isNull()
            | f.isnan(f.col("longitude"))
        )
        for column in ("admin1", "admin1_id_giga", "admin2", "admin2_id_giga"):
            df = df.withColumn(
                column,
                f.when(
                    missing_location_condition, f.lit(None).cast(StringType())
                ).otherwise(f.col(column)),
            )

    # Connectivity govt ingestion timestamp: set when connectivity_govt is uploaded.
    if "connectivity_govt" in uploaded_columns:
        df = df.withColumn(
            "connectivity_govt_ingestion_timestamp",
            f.when(
                f.col("connectivity_govt").isNotNull(), f.current_timestamp()
            ).otherwise(f.lit(None).cast(TimestampType())),
        )

    # RT connectivity columns: merge from the realtime schools table
    if settings.DEPLOY_ENV != DeploymentEnvironment.LOCAL:
        connectivity = get_country_rt_schools(spark, country_code_iso3)
        df = merge_connectivity_to_master(df, connectivity, uploaded_columns)

    if source == "gigameter":
        df = set_school_registration_verification_status(df, uploaded_columns)

    return df


def get_admin_boundaries(
    country_code_iso3: str,
    admin_level: str,
) -> pd.DataFrame | gpd.GeoDataFrame | None:  # admin level = ["admin1", "admin2"]
    try:
        service = get_blob_service_client()
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


def add_admin_columns(
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
                admin_level: f.lit("Unknown"),
                f"{admin_level}_id_giga": f.lit(None).cast(StringType()),
            }
        )

    spark = df.sparkSession
    broadcasted_admin_boundaries = spark.sparkContext.broadcast(admin_boundaries)

    result_schema = StructType(
        [
            StructField("name_en", StringType()),
            StructField("name_native", StringType()),
            StructField("id_giga", StringType()),
        ]
    )

    @pandas_udf(result_schema)
    def get_admin_info(latitude: pd.Series, longitude: pd.Series) -> pd.DataFrame:
        boundaries = broadcasted_admin_boundaries.value
        gdf_points = gpd.GeoDataFrame(
            {"orig_index": range(len(latitude))},
            geometry=gpd.points_from_xy(longitude, latitude),
            crs="epsg:4326",
        )
        joined = gpd.sjoin(gdf_points, boundaries, how="left", predicate="within")
        # sjoin may produce duplicates for points on shared boundaries — keep first match
        joined = joined[~joined.index.duplicated(keep="first")]
        return pd.DataFrame(
            {
                "name_en": joined.get("name_en"),
                "name_native": joined.get("name"),
                "id_giga": joined.get(f"{admin_level}_id_giga"),
            }
        )

    result_col = f"_admin_info_{admin_level}"
    df = df.withColumn(result_col, get_admin_info(df["latitude"], df["longitude"]))

    coalesce_args = [
        f.col(f"{result_col}.name_en"),
        f.col(f"{result_col}.name_native"),
    ]
    if admin_level in df.columns:
        coalesce_args.append(f.col(admin_level))
    coalesce_args.append(f.lit("Unknown"))

    return df.withColumns(
        {
            admin_level: f.coalesce(*coalesce_args),
            f"{admin_level}_id_giga": f.col(f"{result_col}.id_giga"),
        }
    ).drop(result_col)


def add_disputed_region_column(df: sql.DataFrame) -> sql.DataFrame:
    try:
        service = get_blob_service_client()
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
    uploaded_columns: list[str],
):
    connectivity_columns = [
        col
        for col in connectivity.columns
        if col not in ("school_id_giga", "school_id_govt")
    ]
    columns_to_drop = [col for col in connectivity_columns if col in master.columns]
    master = master.drop(*columns_to_drop)

    connectivity = connectivity.withColumnsRenamed(
        {
            "school_id_govt": "school_id_govt_connectivity",
            "school_id_giga": "school_id_giga_connectivity",
        }
    )

    master = master.join(
        connectivity,
        on=[
            f.lower(master.school_id_govt)
            == f.lower(connectivity.school_id_govt_connectivity)
        ],
        how="left",
    )

    master = master.withColumn(
        "connectivity_RT", f.coalesce(f.col("connectivity_RT"), f.lit("No"))
    )

    # standardize connectivity_govt only when it was uploaded; ensure column exists otherwise
    if "connectivity_govt" in uploaded_columns:
        master = master.withColumn(
            "connectivity_govt",
            f.when(
                f.isnan(f.col("connectivity_govt")), f.lit(None).cast(StringType())
            ).otherwise(f.initcap(f.trim(f.col("connectivity_govt")))),
        )
    elif "connectivity_govt" not in master.columns:
        master = master.withColumn("connectivity_govt", f.lit(None).cast(StringType()))

    # determine the value of connectivity
    if {"download_speed_govt", "connectivity_govt"}.issubset(set(uploaded_columns)):
        # this block runs only when both download_speed_govt and connectivity_govt are uploaded;
        # for new schools without those columns, connectivity defaults to "Unknown"

        master = master.withColumn(
            "connectivity",
            f.when(
                (f.lower(f.col("connectivity_RT")) == "yes")
                | (f.lower(f.col("connectivity_govt")) == "yes"),
                "Yes",
            )
            .when(
                f.lower(f.col("connectivity_govt")) == "no",
                "No",
            )
            .otherwise(
                f.when(f.col("is_new_school"), f.lit("Unknown")).otherwise(f.lit(None))
            ),
        )
    elif "connectivity_govt" in uploaded_columns:
        # this will run during updates if connectivity_govt is in the uploaded file without download_speed_govt
        master = master.withColumn(
            "connectivity",
            f.when(f.lower(f.col("connectivity_RT")) == "yes", "Yes")
            .when(f.lower(f.col("connectivity_govt")) == "yes", "Yes")
            .when(f.lower(f.col("connectivity_govt")) == "no", "No")
            .when(f.lower(f.col("connectivity_govt")) == "unknown", "Unknown")
            .otherwise(f.lit(None).cast(StringType())),
        )
    elif "download_speed_govt" in uploaded_columns:
        # this will run during updates if download_speed_govt is in the uploaded file without connectivity_govt
        master = master.withColumn(
            "connectivity",
            f.when(f.lower(f.col("connectivity_RT")) == "yes", "Yes")
            .when(f.col("download_speed_govt") > 0, "Yes")
            .when(f.col("download_speed_govt") == 0, "No")
            .otherwise(f.lit(None).cast(StringType())),
        )

    # add the time connectivity_govt was ingested (only when it was part of the upload)
    if "connectivity_govt" in uploaded_columns:
        master = master.withColumn(
            "connectivity_govt_ingestion_timestamp",
            f.when(
                f.col("connectivity_govt").isNotNull(), f.current_timestamp()
            ).otherwise(f.lit(None).cast(TimestampType())),
        )

    master_cols_to_drop = [
        col
        for col in master.columns
        if col in ("school_id_govt_connectivity", "school_id_giga_connectivity")
    ]

    master = master.drop(*master_cols_to_drop)

    return master


def get_all_connectivity_rt_schools(context, spark: SparkSession, table_exists=True):
    from src.internal.connectivity_queries import (
        get_all_gigameter_schools,
        get_all_mlab_schools,
        get_all_qos_schools,
        get_rt_schools,
    )

    gigameter_schools = get_all_gigameter_schools()
    context.log.info(
        f"Total number of gigameter schools is {gigameter_schools.shape[0]}"
    )
    mlab_schools = get_all_mlab_schools()
    context.log.info(f"Total number of mlab schools is {mlab_schools.shape[0]}")

    qos_schema_tables_df = get_qos_tables()
    qos_schema_tables = qos_schema_tables_df["Table"].to_list()
    qos_countries = [
        table
        for table in qos_schema_tables
        if coco.convert(table, to="short_name") != "not found"
    ]
    context.log.info(f"Fetching QoS data for {len(qos_countries)} countries")
    qos_schools = get_all_qos_schools(qos_countries)
    context.log.info(f"Total number of QoS schools is {qos_schools.shape[0]}")

    gigameter_schools_df = spark.createDataFrame(gigameter_schools)
    mlab_schools_df = spark.createDataFrame(mlab_schools)
    mlab_schools_df = mlab_schools_df.withColumnsRenamed(
        {col: f"{col}_mlab" for col in mlab_schools_df.columns}
    )
    qos_schools_df = spark.createDataFrame(qos_schools)
    qos_schools_df = qos_schools_df.withColumnsRenamed(
        {col: f"{col}_qos" for col in qos_schools_df.columns}
    )

    connectivity_rt_schools = gigameter_schools_df.join(
        mlab_schools_df,
        how="outer",
        on=[
            (gigameter_schools_df.school_id_govt == mlab_schools_df.school_id_govt_mlab)
            & (gigameter_schools_df.country_code == mlab_schools_df.country_code_mlab)
        ],
    )

    connectivity_rt_schools = connectivity_rt_schools.withColumn(
        "school_id_govt",
        f.coalesce(f.col("school_id_govt"), f.col("school_id_govt_mlab")),
    )

    connectivity_rt_schools = connectivity_rt_schools.join(
        qos_schools_df,
        how="outer",
        on=[
            (
                connectivity_rt_schools.school_id_govt
                == qos_schools_df.school_id_govt_qos
            )
            & (connectivity_rt_schools.country_code == qos_schools_df.country_code_qos)
        ],
    )

    connectivity_rt_schools = connectivity_rt_schools.withColumn(
        "school_id_govt",
        f.coalesce(f.col("school_id_govt"), f.col("school_id_govt_qos")),
    )

    # ensure we fill all the values of the columns from across the sources

    connectivity_rt_schools = connectivity_rt_schools.withColumn(
        "school_id_giga",
        f.coalesce(
            f.col("school_id_giga"),
            f.col("school_id_giga_mlab"),
            f.col("school_id_giga_qos"),
        ),
    )

    connectivity_rt_schools = connectivity_rt_schools.withColumn(
        "country_code",
        f.coalesce(
            f.col("country_code"),
            f.col("country_code_mlab"),
            f.col("country_code_qos"),
        ),
    )

    connectivity_rt_schools = connectivity_rt_schools.withColumn(
        "connectivity_RT_ingestion_timestamp",
        f.least(
            f.col("first_measurement_timestamp"),
            f.col("first_measurement_timestamp_mlab"),
            f.col("first_measurement_timestamp_qos"),
        ),
    )

    connectivity_rt_schools = connectivity_rt_schools.withColumn(
        "connectivity_RT_datasource",
        f.regexp_replace(
            f.concat_ws(
                ", ",
                f.trim(f.col("source")),
                f.trim(f.col("source_mlab")),
                f.trim(f.col("source_qos")),
            ),
            "^, |, $",
            "",
        ),
    )

    connectivity_rt_schools = connectivity_rt_schools.withColumn(
        "connectivity_RT", f.lit("Yes")
    )

    columns_to_keep = [
        "school_id_giga",
        "school_id_govt",
        "connectivity_RT",
        "connectivity_RT_ingestion_timestamp",
        "connectivity_RT_datasource",
        "country_code",
    ]
    connectivity_rt_schools = connectivity_rt_schools.select(*columns_to_keep)
    connectivity_rt_schools = connectivity_rt_schools.filter(
        f.col("school_id_giga").isNotNull()
    )

    if not table_exists:
        context.log.info(
            "Creating RT schools table for the first time, adding RT data from giga maps"
        )
        gigamaps_rt_schs = get_rt_schools()
        gigamaps_rt_schs_df = spark.createDataFrame(gigamaps_rt_schs)
        gigamaps_rt_schs_df = gigamaps_rt_schs_df.withColumnsRenamed(
            {col: f"{col}_maps" for col in gigamaps_rt_schs_df.columns}
        )

        context.log.info("Add giga maps RT data to RT schools from all sources")

        connectivity_rt_schools = connectivity_rt_schools.join(
            gigamaps_rt_schs_df,
            how="outer",
            on=[
                (
                    connectivity_rt_schools.school_id_govt
                    == gigamaps_rt_schs_df.school_id_govt_maps
                )
                & (
                    connectivity_rt_schools.country_code
                    == gigamaps_rt_schs_df.country_code_maps
                )
            ],
        )

        connectivity_rt_schools = connectivity_rt_schools.withColumn(
            "school_id_giga",
            f.coalesce(f.col("school_id_giga"), f.col("school_id_giga_maps")),
        )
        connectivity_rt_schools = connectivity_rt_schools.withColumn(
            "school_id_govt",
            f.coalesce(f.col("school_id_govt"), f.col("school_id_govt_maps")),
        )
        connectivity_rt_schools = connectivity_rt_schools.withColumn(
            "connectivity_RT_ingestion_timestamp",
            f.when(
                f.col("country_code_maps") == "BRA",
                f.coalesce(
                    f.col("connectivity_rt_ingestion_timestamp_maps"),
                    f.col("connectivity_RT_ingestion_timestamp"),
                ),
            ).otherwise(
                f.coalesce(
                    f.col("connectivity_rt_ingestion_timestamp"),
                    f.col("connectivity_RT_ingestion_timestamp_maps"),
                )
            ),
        )
        connectivity_rt_schools = connectivity_rt_schools.withColumn(
            "connectivity_RT_datasource",
            f.when(f.col("country_code_maps") == "BRA", f.lit("qos")).otherwise(
                f.col("connectivity_RT_datasource")
            ),
        )
        connectivity_rt_schools = connectivity_rt_schools.withColumn(
            "country_code",
            f.coalesce(f.col("country_code"), f.col("country_code_maps")),
        )
        connectivity_rt_schools = connectivity_rt_schools.withColumn(
            "connectivity_RT", f.lit("Yes")
        )

        connectivity_rt_schools = connectivity_rt_schools.select(*columns_to_keep)
        connectivity_rt_schools = connectivity_rt_schools.filter(
            f.col("connectivity_RT_datasource").isNotNull()
        )

    return connectivity_rt_schools


def get_country_rt_schools(spark: SparkSession, country_code: str) -> sql.DataFrame:
    schema_name = "pipeline_tables"
    table_name = "school_connectivity_realtime_schools"

    full_table_name = construct_full_table_name(schema_name, table_name)

    all_rt_schools = DeltaTable.forName(spark, full_table_name).toDF()
    country_rt_schools = all_rt_schools.filter(f.col("country_code") == country_code)
    country_rt_schools = country_rt_schools.drop("country_code")
    return country_rt_schools


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

    df = row_level_checks(
        df=df,
        silver=silver,
        dataset_type="geolocation",
        _country_code_iso3="BRA",
    )
    df.show()

    df_all, df_approved = aggregate_report_spark_df(spark=df.sparkSession, df=df)
    df_all.show(500)
