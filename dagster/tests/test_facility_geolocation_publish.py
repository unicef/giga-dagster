import pytest
from pyspark.sql import SparkSession

from src.constants import StagingChangeType
from src.facilities import (
    HEALTH_SPEC,
    merge_approved_facilities,
    split_approved_facilities,
    validate_facility_frames,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("facility-geolocation-contract-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _approved(spark, *, change_type, name="Clinic A", bed_count="10"):
    return spark.createDataFrame(
        [
            {
                "facility_id_giga": "facility-1",
                "facility_id_govt": "govt-1",
                "facility_name": name,
                "facility_level": "primary",
                "country_code": "UGA",
                "latitude": "0.31",
                "longitude": "32.58",
                "bed_count": bed_count,
                "change_type": str(change_type),
            }
        ]
    )


def test_split_projects_shared_and_specialized_columns(spark):
    projected = split_approved_facilities(
        _approved(spark, change_type=StagingChangeType.INSERT),
        HEALTH_SPEC,
        allow_missing=True,
    )

    core = projected.core.first().asDict()
    extension = projected.extension.first().asDict()
    assert core["facility_id_giga"] == extension["facility_id_giga"] == "facility-1"
    assert core["facility_type"] == "health"
    assert core["facility_name"] == "Clinic A"
    assert "bed_count" not in core
    assert extension["bed_count"] == "10"
    assert core["core_signature"]
    assert extension["extension_signature"]


def test_dual_table_merge_handles_insert_update_and_delete(spark):
    insert = _approved(spark, change_type=StagingChangeType.INSERT)
    schemas = split_approved_facilities(insert, HEALTH_SPEC, allow_missing=True)
    empty_core = spark.createDataFrame([], schemas.core.schema)
    empty_extension = spark.createDataFrame([], schemas.extension.schema)

    inserted = merge_approved_facilities(
        empty_core,
        empty_extension,
        insert,
        HEALTH_SPEC,
    )
    validate_facility_frames(inserted.core, inserted.extension, HEALTH_SPEC)
    assert inserted.core.first()["facility_name"] == "Clinic A"
    assert inserted.extension.first()["bed_count"] == "10"

    updated = merge_approved_facilities(
        inserted.core,
        inserted.extension,
        _approved(
            spark,
            change_type=StagingChangeType.UPDATE,
            name="Clinic B",
            bed_count="20",
        ),
        HEALTH_SPEC,
    )
    validate_facility_frames(updated.core, updated.extension, HEALTH_SPEC)
    assert updated.core.first()["facility_name"] == "Clinic B"
    assert updated.extension.first()["bed_count"] == "20"

    deleted = merge_approved_facilities(
        updated.core,
        updated.extension,
        _approved(spark, change_type=StagingChangeType.DELETE),
        HEALTH_SPEC,
    )
    validate_facility_frames(deleted.core, deleted.extension, HEALTH_SPEC)
    assert deleted.core.count() == 0
    assert deleted.extension.count() == 0


def test_integrity_validation_rejects_orphan_extension(spark):
    approved = _approved(spark, change_type=StagingChangeType.INSERT)
    projected = split_approved_facilities(
        approved,
        HEALTH_SPEC,
        allow_missing=True,
    )

    with pytest.raises(ValueError, match="orphan"):
        validate_facility_frames(
            spark.createDataFrame([], projected.core.schema),
            projected.extension,
            HEALTH_SPEC,
        )
