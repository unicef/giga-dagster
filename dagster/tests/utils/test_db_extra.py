from unittest.mock import patch

import pytest
from src.utils.db import mlab, proco


def test_mlab_no_connection_string():
    mlab._mlab = None
    with patch("src.utils.db.mlab.settings") as mock_settings:
        mock_settings.MLAB_DB_CONNECTION_STRING = None
        with pytest.raises(
            ValueError, match="MLAB_DB_CONNECTION_STRING is not configured"
        ):
            mlab._get_mlab_provider()


def test_mlab_with_connection_string():
    mlab._mlab = None
    with patch("src.utils.db.mlab.settings") as mock_settings:
        mock_settings.MLAB_DB_CONNECTION_STRING = "postgresql://user:pass@host/db"
        provider = mlab._get_mlab_provider()
        assert provider is not None
        provider2 = mlab._get_mlab_provider()
        assert provider is provider2

        # Test get_db
        with patch.object(provider, "get_db") as mock_get_db:
            mlab.get_db()
            mock_get_db.assert_called_once()

        # Test get_db_context
        with patch.object(provider, "get_db_context") as mock_get_db_context:
            mlab.get_db_context()
            mock_get_db_context.assert_called_once()


def test_proco_no_connection_string():
    proco._proco = None
    with patch("src.utils.db.proco.settings") as mock_settings:
        mock_settings.PROCO_DB_CONNECTION_STRING = None
        with pytest.raises(
            ValueError, match="PROCO_DB_CONNECTION_STRING is not configured"
        ):
            proco._get_proco_provider()


def test_proco_with_connection_string():
    proco._proco = None
    with patch("src.utils.db.proco.settings") as mock_settings:
        mock_settings.PROCO_DB_CONNECTION_STRING = "postgresql://user:pass@host/db"
        provider = proco._get_proco_provider()
        assert provider is not None
        provider2 = proco._get_proco_provider()
        assert provider is provider2

        # Test get_db
        with patch.object(provider, "get_db") as mock_get_db:
            proco.get_db()
            mock_get_db.assert_called_once()

        # Test get_db_context
        with patch.object(provider, "get_db_context") as mock_get_db_context:
            proco.get_db_context()
            mock_get_db_context.assert_called_once()
