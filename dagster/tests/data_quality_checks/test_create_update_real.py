from pyspark.sql import Row
from src.data_quality_checks.create_update import create_checks, update_checks


def test_create_checks(spark_session):
    bronze_data = [Row(school_id_govt="new_1", school_id_giga="g1")]
    silver_data = [Row(school_id_govt="old_1", school_id_giga="g2")]
    bronze = spark_session.createDataFrame(bronze_data)
    silver = spark_session.createDataFrame(silver_data)
    res = create_checks(bronze, silver)
    rows = res.collect()
    assert rows[0]["dq_is_not_create"] == 0


def test_create_checks_exists(spark_session):
    bronze_data = [Row(school_id_govt="old_1", school_id_giga="g1")]
    silver_data = [Row(school_id_govt="old_1", school_id_giga="g1")]
    bronze = spark_session.createDataFrame(bronze_data)
    silver = spark_session.createDataFrame(silver_data)
    res = create_checks(bronze, silver)
    rows = res.collect()
    assert rows[0]["dq_is_not_create"] == 1


def test_update_checks(spark_session):
    bronze_data = [Row(school_id_govt="u1", school_id_giga="g1")]
    silver_data = [Row(school_id_govt="u1", school_id_giga="g1")]
    bronze = spark_session.createDataFrame(bronze_data)
    silver = spark_session.createDataFrame(silver_data)
    res = update_checks(bronze, silver)
    rows = res.collect()
    assert rows[0]["dq_is_not_update"] == 0


def test_update_checks_missing(spark_session):
    bronze_data = [Row(school_id_govt="u2")]
    silver_data = [Row(school_id_govt="u1")]
    bronze = spark_session.createDataFrame(bronze_data)
    silver = spark_session.createDataFrame(silver_data)
    res = update_checks(bronze, silver)
    rows = res.collect()
    assert rows[0]["dq_is_not_update"] == 1
