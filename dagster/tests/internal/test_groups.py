from unittest.mock import MagicMock, patch

from src.internal.groups import GroupsApi


def test_list_role_members():
    with patch("src.internal.groups.get_db_context") as mock_db_ctx:
        mock_db = MagicMock()
        mock_db_ctx.return_value.__enter__.return_value = mock_db

        mock_user1 = MagicMock()
        mock_user1.email = "user1@example.com"
        mock_user2 = MagicMock()
        mock_user2.email = "user2@example.com"

        mock_db.scalars.return_value = [mock_user1, mock_user2]

        emails = GroupsApi.list_role_members("Admin")

        assert len(emails) == 2
        assert "user1@example.com" in emails
        assert "user2@example.com" in emails

        mock_db.scalars.assert_called()


def test_list_country_role_members():
    with patch("src.internal.groups.get_db_context") as mock_db_ctx:
        mock_db = MagicMock()
        mock_db_ctx.return_value.__enter__.return_value = mock_db

        mock_db.scalars.return_value = ["user1@example.com"]

        with patch("src.internal.groups.coco.convert", return_value="CountryName"):
            emails = GroupsApi.list_country_role_members("CTY")

            assert emails == ["user1@example.com"]
            mock_db.scalars.assert_called()
