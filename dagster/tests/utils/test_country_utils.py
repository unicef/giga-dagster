from unittest.mock import MagicMock, patch

from src.utils.country import get_country_codes_list


@patch("src.utils.country.CountryConverter")
def test_get_country_codes_list(mock_coco):
    mock_instance = MagicMock()
    mock_coco.return_value = mock_instance
    mock_series = MagicMock()
    mock_series.to_list.return_value = ["BRA", "USA"]
    mock_instance.data = {"ISO3": mock_series}
    assert get_country_codes_list() == ["BRA", "USA"]
