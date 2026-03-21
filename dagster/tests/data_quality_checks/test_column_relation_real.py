import pytest
from pyspark.sql import Row
from pyspark.sql.functions import lit
from src.data_quality_checks.column_relation import column_relation_checks


@pytest.fixture(scope="module")
def spark_session_local(spark_session):
    return spark_session


def test_column_relation_checks_master(spark_session):
    data = [
        Row(connectivity="yes", connectivity_RT="yes"),
        Row(connectivity="yes", connectivity_RT="no"),
    ]
    df = spark_session.createDataFrame(data)
    cols = [
        "connectivity_govt",
        "download_speed_contracted",
        "connectivity_RT_datasource",
        "connectivity_RT_ingestion_timestamp",
        "cellular_coverage_availability",
        "cellular_coverage_type",
        "connectivity_govt_ingestion_timestamp",
        "electricity_availability",
        "electricity_type",
    ]
    for c in cols:
        df = df.withColumn(c, lit(None).cast("string"))
    res = column_relation_checks(df, "master")

    assert (
        "dq_column_relation_checks-connectivity_connectivity_RT_connectivity_govt_download_speed_contracted"
        in res.columns
    )


def test_column_relation_checks_coverage(spark_session):
    data = [
        Row(nearest_NR_id="id1", dist_5g="10.0"),
        Row(nearest_NR_id=None, dist_5g="10.0"),
    ]
    df = spark_session.createDataFrame(data)
    df = df.withColumnRenamed("dist_5g", "5g_cell_site_dist")
    other_cols = [
        "nearest_LTE_id",
        "4g_cell_site_dist",
        "nearest_UMTS_id",
        "3g_cell_site_dist",
        "nearest_GSM_id",
        "2g_cell_site_dist",
    ]
    for c in other_cols:
        df = df.withColumn(c, lit(None).cast("string"))
    res = column_relation_checks(df, "coverage")
    rows = res.collect()
    col_name = "dq_column_relation_checks-nearest_NR_id_5g_cell_site_dist"
    assert col_name in res.columns
    assert rows[0][col_name] == 0
    assert rows[1][col_name] == 1
