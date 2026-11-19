from pyspark.sql import (
    Row,
    functions as f,
    types as T,
)
from src.data_quality_checks.facility import prepare_facility_bronze
from src.facilities import HEALTH_SPEC, SCHOOL_SPEC
from src.settings import DeploymentEnvironment
from src.spark.transform_functions import (
    _normalize_connectivity_identity_columns,
    merge_connectivity_to_master,
)


def _session():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[1]")
        .appName("facility-bronze-enrichment-tests")
        .getOrCreate()
    )


def test_normalize_connectivity_identity_renames_school_columns():
    spark = _session()
    connectivity = spark.createDataFrame(
        [Row(school_id_govt="A1", school_id_giga="g1", connectivity_RT="Yes")]
    )
    normalized = _normalize_connectivity_identity_columns(connectivity)
    assert "facility_id_govt" in normalized.columns
    assert "facility_id_giga" in normalized.columns
    assert "school_id_govt" not in normalized.columns
    assert "school_id_giga" not in normalized.columns


def test_merge_connectivity_joins_on_facility_id_govt():
    spark = _session()
    master = spark.createDataFrame(
        [
            Row(
                facility_id_govt="A1",
                is_new_facility=True,
                connectivity_govt="No",
            )
        ]
    )
    connectivity = spark.createDataFrame(
        [
            Row(
                school_id_govt="A1",
                school_id_giga="g1",
                connectivity_RT="Yes",
                connectivity_RT_datasource="gigameter",
            )
        ]
    )
    merged = merge_connectivity_to_master(
        master, connectivity, uploaded_columns=["connectivity_govt"]
    )
    row = merged.collect()[0]
    assert row.connectivity_RT == "Yes"
    assert row.connectivity == "Yes"
    assert row.connectivity_RT_datasource == "gigameter"
    assert "facility_id_govt_connectivity" not in merged.columns


def test_prepare_facility_bronze_sets_connectivity_govt_timestamp(monkeypatch):
    spark = _session()
    monkeypatch.setattr(
        "src.data_quality_checks.facility.map_govt_to_giga_columns",
        lambda df, uploaded_columns: df,
    )
    monkeypatch.setattr(
        "src.data_quality_checks.facility.add_connectivity_type_columns",
        lambda df, uploaded_columns: df,
    )
    monkeypatch.setattr(
        "src.data_quality_checks.facility.add_admin_columns",
        lambda df, country_code, admin_level: df,
    )
    monkeypatch.setattr(
        "src.data_quality_checks.facility.add_disputed_region_column",
        lambda df: df,
    )

    schema = T.StructType(
        [
            T.StructField("facility_id_govt", T.StringType(), True),
            T.StructField("facility_name", T.StringType(), True),
            T.StructField("facility_level", T.StringType(), True),
            T.StructField("country_code", T.StringType(), True),
            T.StructField("geo_latitude", T.StringType(), True),
            T.StructField("geo_longitude", T.StringType(), True),
            T.StructField("connectivity_govt", T.StringType(), True),
        ]
    )
    df = spark.createDataFrame(
        [
            (
                "HC-1",
                "Clinic",
                "Primary",
                "UGA",
                "0.1",
                "32.1",
                "Yes",
            )
        ],
        schema=schema,
    )
    current = spark.createDataFrame([], schema=schema)
    # Force LOCAL so RT merge is skipped without a realtime table.
    monkeypatch.setattr(
        "src.data_quality_checks.facility.settings.DEPLOY_ENV",
        DeploymentEnvironment.LOCAL,
    )
    result = prepare_facility_bronze(
        df,
        current,
        HEALTH_SPEC,
        "UGA",
        uploaded_columns=list(df.columns),
        source=None,
        spark=spark,
    )
    assert "connectivity_govt_ingestion_timestamp" in result.columns
    assert "facility_id_giga" in result.columns
    assert "is_new_facility" in result.columns


def test_prepare_facility_bronze_registration_only_for_school_gigameter(monkeypatch):
    spark = _session()
    calls = {"school": 0}

    monkeypatch.setattr(
        "src.data_quality_checks.facility.map_govt_to_giga_columns",
        lambda df, uploaded_columns: df,
    )
    monkeypatch.setattr(
        "src.data_quality_checks.facility.add_connectivity_type_columns",
        lambda df, uploaded_columns: df,
    )
    monkeypatch.setattr(
        "src.data_quality_checks.facility.add_admin_columns",
        lambda df, country_code, admin_level: df,
    )
    monkeypatch.setattr(
        "src.data_quality_checks.facility.add_disputed_region_column",
        lambda df: df,
    )

    def fake_registration(df, uploaded_columns):
        calls["school"] += 1
        return df.withColumn("verification_status", f.lit("unverified"))

    monkeypatch.setattr(
        "src.data_quality_checks.facility.set_school_registration_verification_status",
        fake_registration,
    )
    monkeypatch.setattr(
        "src.data_quality_checks.facility.settings.DEPLOY_ENV",
        DeploymentEnvironment.LOCAL,
    )

    school_df = spark.createDataFrame(
        [
            Row(
                facility_id_govt="S1",
                facility_name="School",
                facility_level="Primary",
                country_code="UGA",
                geo_latitude="0.1",
                geo_longitude="32.1",
            )
        ]
    )
    current = spark.createDataFrame([], schema=school_df.schema)
    school_result = prepare_facility_bronze(
        school_df,
        current,
        SCHOOL_SPEC,
        "UGA",
        uploaded_columns=list(school_df.columns),
        source="gigameter",
        spark=spark,
    )
    assert calls["school"] == 1
    assert "verification_status" in school_result.columns

    health_df = spark.createDataFrame(
        [
            Row(
                facility_id_govt="H1",
                facility_name="Clinic",
                facility_level="Primary",
                country_code="UGA",
                geo_latitude="0.1",
                geo_longitude="32.1",
            )
        ]
    )
    health_current = spark.createDataFrame([], schema=health_df.schema)
    prepare_facility_bronze(
        health_df,
        health_current,
        HEALTH_SPEC,
        "UGA",
        uploaded_columns=list(health_df.columns),
        source="gigameter",
        spark=spark,
    )
    assert calls["school"] == 1
