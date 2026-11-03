import os
from unittest.mock import MagicMock

import pandas as pd
import pytest

# src.settings validates these at import time; tests must not need a real .env.
for _name in (
    "AZURE_SAS_TOKEN",
    "AZURE_BLOB_CONTAINER_NAME",
    "AZURE_STORAGE_ACCOUNT_NAME",
    "SLACK_WEBHOOK",
    "SPARK_RPC_AUTHENTICATION_SECRET",
    "HIVE_METASTORE_URI",
    "EMAIL_RENDERER_BEARER_TOKEN",
    "INGESTION_POSTGRESQL_USERNAME",
    "INGESTION_POSTGRESQL_PASSWORD",
    "INGESTION_POSTGRESQL_DATABASE",
):
    os.environ.setdefault(_name, "test")

from src.utils import map_generator  # noqa: E402


@pytest.fixture
def schools():
    return pd.DataFrame(
        {
            "school_id_giga": ["a", "b"],
            "latitude": [-1.29, -1.30],
            "longitude": [36.82, 36.83],
        }
    )


def test_basemap_tiles_url_appends_encoded_key():
    url = map_generator._basemap_tiles_url("ab c/&d")

    assert url == f"{map_generator.BASEMAP_TILES_URL}?key=ab%20c%2F%26d"


def test_basemap_tiles_url_without_key():
    assert map_generator._basemap_tiles_url("") == map_generator.BASEMAP_TILES_URL


def test_generate_school_map_html_uses_carto_key(monkeypatch, schools):
    monkeypatch.setattr(map_generator.settings, "CARTO_BASEMAP_API_KEY", "test-key")
    context = MagicMock()

    html = map_generator.generate_school_map_html(
        "KEN", schools, schools.iloc[0:0], context
    )

    assert "rastertiles/light_all/{z}/{x}/{y}.png?key=test-key" in html
    assert "carto.com/attributions" in html
    context.log.warning.assert_not_called()


def test_generate_school_map_html_warns_without_carto_key(monkeypatch, schools):
    monkeypatch.setattr(map_generator.settings, "CARTO_BASEMAP_API_KEY", "")
    context = MagicMock()

    html = map_generator.generate_school_map_html(
        "KEN", schools, schools.iloc[0:0], context
    )

    assert "?key=" not in html
    context.log.warning.assert_called_once()
