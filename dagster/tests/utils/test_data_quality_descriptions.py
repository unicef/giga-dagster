from unittest.mock import MagicMock, patch

import pytest
from src.utils.data_quality_descriptions import (
    convert_dq_checks_to_human_readeable_descriptions_and_upload,
    handle_rename_dq_has_critical_error_column,
    human_readable_coverage_coverage_itu_checks,
    human_readable_coverage_fb_checks,
    human_readable_geolocation_checks,
    human_readable_standard_checks,
)


@pytest.fixture
def mock_config():
    with patch("src.utils.data_quality_descriptions.Config") as MockConfig:
        config_instance = MockConfig.return_value
        config_instance.UNIQUE_COLUMNS_MASTER = ["col1"]
        config_instance.NONEMPTY_COLUMNS_ALL = ["col2"]
        config_instance.VALUES_DOMAIN_ALL = {"col3": ["val1", "val2"]}
        config_instance.VALUES_RANGE_ALL = {"col4": {"min": 0, "max": 10}}
        config_instance.DATA_TYPES = {("col5", "STRING")}
        config_instance.PRECISION = {"col6": {"min": 2}}
        config_instance.UNIQUE_SET_COLUMNS = [["col7", "col8"]]
        config_instance.NONEMPTY_COLUMNS_COVERAGE = ["cov_col1"]
        config_instance.NONEMPTY_COLUMNS_COVERAGE_ITU = ["itu_col1"]
        config_instance.NONEMPTY_COLUMNS_COVERAGE_FB = ["fb_col1"]
        yield config_instance


def test_human_readable_standard_checks(mock_config):
    columns = ["extra_col"]
    result = human_readable_standard_checks(columns)
    assert isinstance(result, dict)
    assert "dq_duplicate-col1" in result
    assert "dq_is_null_optional-col2" in result


def test_human_readable_geolocation_checks(mock_config):
    result = human_readable_geolocation_checks()
    assert isinstance(result, dict)
    assert "dq_is_not_within_country" in result
    assert "dq_precision-col6" in result


def test_human_readable_coverage_checks(mock_config):
    itu = human_readable_coverage_coverage_itu_checks()
    assert isinstance(itu, dict)
    fb = human_readable_coverage_fb_checks()
    assert isinstance(fb, dict)


def test_convert_dq_checks_upload(spark_session, mock_config):
    with patch("src.utils.data_quality_descriptions.ADLSFileClient") as MockADLS:
        mock_client = MockADLS.return_value
        data = [("id1", 1, 0)]
        dq_results = spark_session.createDataFrame(
            data, ["id", "dq_duplicate-col1", "dq_is_null_optional-col2"]
        )
        bronze_data = [("val",) * 8]
        bronze_df = spark_session.createDataFrame(
            bronze_data,
            ["col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8"],
        )
        file_config = MagicMock()
        file_config.destination_filepath = (
            "adls://container/raw/geolocation/BRA/file.csv"
        )
        context = MagicMock()
        res_pandas = convert_dq_checks_to_human_readeable_descriptions_and_upload(
            dq_results, "geolocation", bronze_df, file_config, context
        )
        assert res_pandas is not None
        desc_col = "Does the column col1 contain unique values"
        assert desc_col in res_pandas.columns
        assert res_pandas.iloc[0][desc_col] == "No"
        dq_results_cov = spark_session.createDataFrame([("id1",)], ["id"])
        convert_dq_checks_to_human_readeable_descriptions_and_upload(
            dq_results_cov, "coverage", bronze_df, file_config, context
        )
        mock_client.upload_pandas_dataframe_as_file.assert_called()


def test_handle_rename_dq_critical(mock_config):
    cols = ["mand_col"]
    result = handle_rename_dq_has_critical_error_column(cols)
    assert "dq_is_null_mandatory-mand_col" in result
