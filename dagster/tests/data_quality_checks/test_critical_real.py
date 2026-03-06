from unittest.mock import patch

from pyspark.sql import (
    Row,
    functions as f,
)
from src.constants import UploadMode
from src.data_quality_checks.critical import critical_error_checks


def test_critical_error_checks_logic(spark_session):
    data = [
        Row(id="1", lat=10.0, lon=20.0, school_id_giga="g1"),
        Row(id=None, lat=10.0, lon=20.0, school_id_giga="g2"),
    ]
    df = spark_session.createDataFrame(data)
    required_cols = [
        "dq_is_null_mandatory-id",
        "dq_duplicate-school_id_govt",
        "dq_duplicate-school_id_giga",
        "dq_is_null_mandatory-latitude",
        "dq_is_null_mandatory-longitude",
        "dq_is_invalid_range-latitude",
        "dq_is_invalid_range-longitude",
        "dq_is_not_within_country",
        "dq_is_not_create",
    ]
    df_with_dq = df
    for col in required_cols:
        if col == "dq_is_null_mandatory-id":
            df_with_dq = df_with_dq.withColumn(col, (df["id"].isNull()).cast("int"))
        else:
            df_with_dq = df_with_dq.withColumn(col, f.lit(0))
    with patch(
        "src.data_quality_checks.critical.handle_rename_dq_has_critical_error_column",
        return_value={},
    ):
        res = critical_error_checks(
            df_with_dq,
            dataset_type="master",
            config_column_list=["id"],
            mode=UploadMode.CREATE.value,
        )
        rows = res.sort("school_id_giga").collect()
        assert rows[0]["dq_has_critical_error"] == 0
        assert rows[1]["dq_has_critical_error"] == 1
        assert (
            "dq_is_null_mandatory-id" in rows[1]["failure_reason"]
            or "id" in rows[1]["failure_reason"]
        )
