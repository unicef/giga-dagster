"""Tests for the Delta Lake column rename/delete detection helpers in src.utils.delta."""

from src.utils.delta import _detect_renames_and_deletes


class TestDetectRenamesAndDeletes:
    """Unit tests for _detect_renames_and_deletes."""

    def test_no_changes(self):
        existing = {"col_a": "id-1", "col_b": "id-2", "col_c": "id-3"}
        updated = {"col_a": "id-1", "col_b": "id-2", "col_c": "id-3"}
        renames, deletes = _detect_renames_and_deletes(existing, updated)
        assert renames == {}
        assert deletes == []

    def test_column_renamed(self):
        existing = {"old_name": "id-1", "col_b": "id-2"}
        updated = {"new_name": "id-1", "col_b": "id-2"}
        renames, deletes = _detect_renames_and_deletes(existing, updated)
        assert renames == {"old_name": "new_name"}
        assert deletes == []

    def test_column_deleted(self):
        existing = {"col_a": "id-1", "col_b": "id-2", "col_c": "id-3"}
        updated = {"col_a": "id-1", "col_b": "id-2"}
        renames, deletes = _detect_renames_and_deletes(existing, updated)
        assert renames == {}
        assert deletes == ["col_c"]

    def test_column_added_only(self):
        """Adding a column (ID in updated but not existing) should not trigger renames or deletes."""
        existing = {"col_a": "id-1"}
        updated = {"col_a": "id-1", "col_new": "id-new"}
        renames, deletes = _detect_renames_and_deletes(existing, updated)
        assert renames == {}
        assert deletes == []

    def test_rename_and_delete_combined(self):
        existing = {
            "old_name": "id-1",
            "col_b": "id-2",
            "col_to_drop": "id-3",
        }
        updated = {
            "new_name": "id-1",
            "col_b": "id-2",
        }
        renames, deletes = _detect_renames_and_deletes(existing, updated)
        assert renames == {"old_name": "new_name"}
        assert deletes == ["col_to_drop"]

    def test_rename_delete_and_add(self):
        existing = {
            "old_name": "id-1",
            "col_b": "id-2",
            "col_drop": "id-3",
        }
        updated = {
            "new_name": "id-1",
            "col_b": "id-2",
            "col_new": "id-4",
        }
        renames, deletes = _detect_renames_and_deletes(existing, updated)
        assert renames == {"old_name": "new_name"}
        assert deletes == ["col_drop"]

    def test_multiple_renames(self):
        existing = {"a": "id-1", "b": "id-2", "c": "id-3"}
        updated = {"x": "id-1", "y": "id-2", "c": "id-3"}
        renames, deletes = _detect_renames_and_deletes(existing, updated)
        assert renames == {"a": "x", "b": "y"}
        assert deletes == []

    def test_multiple_deletes(self):
        existing = {"a": "id-1", "b": "id-2", "c": "id-3"}
        updated = {"a": "id-1"}
        renames, deletes = _detect_renames_and_deletes(existing, updated)
        assert renames == {}
        assert sorted(deletes) == ["b", "c"]

    def test_empty_existing(self):
        """If existing is empty, there should be no changes."""
        renames, deletes = _detect_renames_and_deletes({}, {"a": "id-1"})
        assert renames == {}
        assert deletes == []

    def test_empty_updated_deletes_all(self):
        """If updated is empty, all existing columns should be deleted."""
        existing = {"a": "id-1", "b": "id-2"}
        renames, deletes = _detect_renames_and_deletes(existing, {})
        assert renames == {}
        assert sorted(deletes) == ["a", "b"]

    def test_both_empty(self):
        renames, deletes = _detect_renames_and_deletes({}, {})
        assert renames == {}
        assert deletes == []
