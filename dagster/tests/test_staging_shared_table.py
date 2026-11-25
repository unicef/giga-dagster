from pyspark.sql import (
    SparkSession,
    functions as f,
)
from src.internal.common_assets.staging import StagingStep


def _step(*, shared: bool) -> StagingStep:
    step = StagingStep.__new__(StagingStep)
    step.is_shared_staging = shared
    step.country_code = "BRA"
    return step


def _session() -> SparkSession:
    return SparkSession.builder.master("local[1]").getOrCreate()


def test_shared_staging_scope_filters_by_country():
    spark = _session()
    rows = spark.createDataFrame(
        [("BRA", "u1"), ("UGA", "u1"), ("BRA", "u2")],
        "country_code string, upload_id string",
    )
    scoped = rows.filter(_step(shared=True)._staging_scope(f.col("upload_id") == "u1"))
    assert [tuple(r) for r in scoped.collect()] == [("BRA", "u1")]


def test_per_country_staging_scope_is_unchanged():
    spark = _session()
    rows = spark.createDataFrame(
        [("BRA", "u1"), ("UGA", "u1")], "country_code string, upload_id string"
    )
    scoped = rows.filter(_step(shared=False)._staging_scope(f.col("upload_id") == "u1"))
    assert scoped.count() == 2
