from types import SimpleNamespace

from src.assets.common import assets as common_assets


class _Rows:
    def __init__(self):
        self.limit_value = None
        self.filter_value = None

    def limit(self, value):
        self.limit_value = value
        return self

    def filter(self, value):
        self.filter_value = value
        return self


def test_parse_approval_file_preserves_portal_change_ids():
    payload = {
        "upload_id": "upload-123",
        "approved_change_ids": ["change-a", "change-b"],
        "rejected_change_ids": ["change-c"],
        "approval_request_log_id": "log-123",
    }

    assert common_assets._parse_approval_file(payload) == (
        "upload-123",
        ["change-a", "change-b"],
        ["change-c"],
        "log-123",
    )
    assert common_assets._parse_approval_file({}) == ("", [], [], None)


def test_resolve_change_ids_supports_all_none_and_selected(monkeypatch):
    rows = _Rows()
    spark_context = SimpleNamespace(broadcast=lambda values: SimpleNamespace(value=values))

    assert common_assets._resolve_change_ids(
        rows, [common_assets._APPROVE_ALL], spark_context
    ) is rows

    assert common_assets._resolve_change_ids(rows, [], spark_context) is rows
    assert rows.limit_value == 0

    selected_expression = object()
    column = SimpleNamespace(isin=lambda values: (selected_expression, values))
    monkeypatch.setattr(
        common_assets,
        "f",
        SimpleNamespace(col=lambda name: column if name == "change_id" else None),
    )

    assert common_assets._resolve_change_ids(
        rows, ["change-a", "change-b"], spark_context
    ) is rows
    assert rows.filter_value == (selected_expression, ["change-a", "change-b"])
