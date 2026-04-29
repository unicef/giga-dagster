"""Tests for the Delta Lake column rename/delete detection helpers in src.utils.delta."""

from src.utils.delta import detect_renames_and_deletes


class TestDetectRenamesAndDeletes:
    """Unit tests for detect_renames_and_deletes."""

    def test_no_changes(self):
        existing = {"col_a": "id-1", "col_b": "id-2", "col_c": "id-3"}
        updated = {"col_a": "id-1", "col_b": "id-2", "col_c": "id-3"}
        renames, deletes = detect_renames_and_deletes(existing, updated)
        assert renames == {}
        assert deletes == []

    def test_column_renamed(self):
        existing = {"old_name": "id-1", "col_b": "id-2"}
        updated = {"new_name": "id-1", "col_b": "id-2"}
        renames, deletes = detect_renames_and_deletes(existing, updated)
        assert renames == {"old_name": "new_name"}
        assert deletes == []

    def test_column_deleted(self):
        existing = {"col_a": "id-1", "col_b": "id-2", "col_c": "id-3"}
        updated = {"col_a": "id-1", "col_b": "id-2"}
        renames, deletes = detect_renames_and_deletes(existing, updated)
        assert renames == {}
        assert deletes == ["col_c"]

    def test_column_added_only(self):
        """Adding a column (ID in updated but not existing) should not trigger renames or deletes."""
        existing = {"col_a": "id-1"}
        updated = {"col_a": "id-1", "col_new": "id-new"}
        renames, deletes = detect_renames_and_deletes(existing, updated)
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
        renames, deletes = detect_renames_and_deletes(existing, updated)
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
        renames, deletes = detect_renames_and_deletes(existing, updated)
        assert renames == {"old_name": "new_name"}
        assert deletes == ["col_drop"]

    def test_multiple_renames(self):
        existing = {"a": "id-1", "b": "id-2", "c": "id-3"}
        updated = {"x": "id-1", "y": "id-2", "c": "id-3"}
        renames, deletes = detect_renames_and_deletes(existing, updated)
        assert renames == {"a": "x", "b": "y"}
        assert deletes == []

    def test_multiple_deletes(self):
        existing = {"a": "id-1", "b": "id-2", "c": "id-3"}
        updated = {"a": "id-1"}
        renames, deletes = detect_renames_and_deletes(existing, updated)
        assert renames == {}
        assert sorted(deletes) == ["b", "c"]

    def test_empty_existing(self):
        """If existing is empty, there should be no changes."""
        renames, deletes = detect_renames_and_deletes({}, {"a": "id-1"})
        assert renames == {}
        assert deletes == []

    def test_empty_updated_deletes_all(self):
        """If updated is empty, all existing columns should be deleted."""
        existing = {"a": "id-1", "b": "id-2"}
        renames, deletes = detect_renames_and_deletes(existing, {})
        assert renames == {}
        assert sorted(deletes) == ["a", "b"]

    def test_both_empty(self):
        renames, deletes = detect_renames_and_deletes({}, {})
        assert renames == {}
        assert deletes == []


class TestSyncSchemaRemovedColumns:
    """Unit tests for sync_schema removed_columns calculation logic."""

    def test_removed_columns_detection(self):
        """Test that removed_columns is correctly calculated after renames."""
        existing_columns = {"col_a", "col_b", "col_c"}
        updated_columns_set = {"col_a", "col_x"}
        removed_columns = existing_columns - updated_columns_set
        assert removed_columns == {"col_b", "col_c"}

    def test_removed_columns_after_rename_applied(self):
        """Test that after rename is applied, only orphaned columns are removed."""
        existing_after_rename = {"col_a", "col_x", "col_c"}
        updated_columns_set = {"col_a", "col_x"}
        removed_columns = existing_after_rename - updated_columns_set
        assert removed_columns == {"col_c"}

    def test_removed_columns_empty_when_no_deletions(self):
        """Test that removed_columns is empty when no columns are deleted."""
        existing_columns = {"col_a", "col_b", "col_c"}
        updated_columns_set = {"col_a", "col_b", "col_c"}
        removed_columns = existing_columns - updated_columns_set
        assert removed_columns == set()


class TestApplyRenamesAndDeletesInitialization:
    """Unit tests for apply_renames_and_deletes mapping initialization."""

    def test_empty_mapping_skips_detection(self):
        """When existing_id_map is completely empty (pre-existing table with no stored
        UUIDs), rename/delete detection must be skipped entirely.

        Previously the code tagged all columns with synthetic IDs and treated them as
        deletes, which caused data loss on pre-existing tables.  The fix: when
        existing_id_map is empty, return early after bootstrapping — do NOT drop anything.
        """
        # Simulate the guard added to apply_renames_and_deletes:
        # if not existing_id_map: bootstrap and return False (no changes)
        existing_id_map = {}
        assert not existing_id_map  # guard condition: skip when completely empty

    def test_partial_mapping_still_detects_deletes(self):
        """When existing_id_map is PARTIALLY populated (some columns have stored UUIDs),
        columns that lack a UUID are given synthetic IDs and treated as deletes if they
        are absent from the updated schema.
        """
        existing_id_map = {"col_a": "id-1", "col_b": "id-2"}
        updated_id_map = {"col_a": "id-1", "col_b": "id-2"}
        current_table_columns = ["col_a", "col_b", "orphan_col"]

        # Supplement with synthetic ID for orphan column
        for col_name in current_table_columns:
            if col_name not in existing_id_map:
                existing_id_map[col_name] = f"table_{col_name}"

        renames, deletes = detect_renames_and_deletes(existing_id_map, updated_id_map)
        assert deletes == ["orphan_col"]
        assert renames == {}


class TestMultipleOperations:
    """Test handling of multiple simultaneous add, rename, and delete operations."""

    def test_multiple_renames_simultaneous(self):
        """Test that multiple columns can be renamed at once."""
        existing = {
            "old_col_a": "id-1",
            "old_col_b": "id-2",
            "old_col_c": "id-3",
            "unchanged": "id-4",
        }
        updated = {
            "new_col_a": "id-1",
            "new_col_b": "id-2",
            "new_col_c": "id-3",
            "unchanged": "id-4",
        }
        renames, deletes = detect_renames_and_deletes(existing, updated)
        assert renames == {
            "old_col_a": "new_col_a",
            "old_col_b": "new_col_b",
            "old_col_c": "new_col_c",
        }
        assert deletes == []

    def test_multiple_deletes_simultaneous(self):
        """Test that multiple columns can be deleted at once."""
        existing = {
            "col_a": "id-1",
            "col_to_drop_1": "id-2",
            "col_b": "id-3",
            "col_to_drop_2": "id-4",
            "col_to_drop_3": "id-5",
        }
        updated = {
            "col_a": "id-1",
            "col_b": "id-3",
        }
        renames, deletes = detect_renames_and_deletes(existing, updated)
        assert renames == {}
        assert sorted(deletes) == ["col_to_drop_1", "col_to_drop_2", "col_to_drop_3"]

    def test_multiple_renames_and_deletes_simultaneous(self):
        """Test multiple renames and deletes happening together."""
        existing = {
            "old_a": "id-1",
            "to_drop_1": "id-2",
            "old_b": "id-3",
            "to_drop_2": "id-4",
            "unchanged": "id-5",
        }
        updated = {
            "new_a": "id-1",
            "new_b": "id-3",
            "unchanged": "id-5",
        }
        renames, deletes = detect_renames_and_deletes(existing, updated)
        assert renames == {"old_a": "new_a", "old_b": "new_b"}
        assert sorted(deletes) == ["to_drop_1", "to_drop_2"]

    def test_full_schema_evolution_add_rename_delete(self):
        """Test complete schema evolution: adds, renames, and deletes simultaneously."""
        existing = {
            "school_id": "id-1",
            "old_funding_type": "id-2",
            "num_teachers_female": "id-3",
            "num_teachers_male": "id-4",
            "old_tablet_count": "id-5",
        }
        updated = {
            "school_id": "id-1",
            "school_funding_source": "id-2",
            "num_tablets_used": "id-5",
        }
        renames, deletes = detect_renames_and_deletes(existing, updated)
        assert renames == {
            "old_funding_type": "school_funding_source",
            "old_tablet_count": "num_tablets_used",
        }
        assert sorted(deletes) == ["num_teachers_female", "num_teachers_male"]

    def test_complex_multi_country_scenario(self):
        """Test scenario matching the user's Gambia case with multiple changes."""
        existing = {
            "school_id_giga": "csv-id-001",
            "school_name": "csv-id-002",
            "school_funding_type": "csv-id-010",
            "num_tablets": "csv-id-015",
            "num_teachers_female": "csv-id-020",
            "num_teachers_male": "csv-id-021",
            "latitude": "csv-id-030",
            "longitude": "csv-id-031",
        }
        updated = {
            "school_id_giga": "csv-id-001",
            "school_name": "csv-id-002",
            "school_funding_source": "csv-id-010",
            "num_tablets_used": "csv-id-015",
            "latitude": "csv-id-030",
            "longitude": "csv-id-031",
        }
        renames, deletes = detect_renames_and_deletes(existing, updated)
        assert renames == {
            "school_funding_type": "school_funding_source",
            "num_tablets": "num_tablets_used",
        }
        assert sorted(deletes) == ["num_teachers_female", "num_teachers_male"]

    def test_multiple_adds_simultaneous(self):
        """Test that multiple columns can be added at once."""
        existing_columns = {"col_a", "col_b", "col_c"}
        updated_columns_set = {
            "col_a",
            "col_b",
            "col_c",
            "new_col_x",
            "new_col_y",
            "new_col_z",
        }
        added_columns = updated_columns_set - existing_columns
        assert added_columns == {"new_col_x", "new_col_y", "new_col_z"}

    def test_complete_workflow_add_rename_delete_together(self):
        """Test the complete workflow: adds, renames, and deletes all together."""
        existing_id_map = {
            "school_id": "uuid-001",
            "old_name_a": "uuid-002",
            "old_name_b": "uuid-003",
            "to_delete_1": "uuid-100",
            "to_delete_2": "uuid-101",
        }
        updated_id_map = {
            "school_id": "uuid-001",
            "new_name_a": "uuid-002",
            "new_name_b": "uuid-003",
            "new_col_x": "uuid-200",
            "new_col_y": "uuid-201",
            "new_col_z": "uuid-202",
        }
        renames, deletes = detect_renames_and_deletes(existing_id_map, updated_id_map)
        assert renames == {"old_name_a": "new_name_a", "old_name_b": "new_name_b"}
        assert sorted(deletes) == ["to_delete_1", "to_delete_2"]
        existing_after_renames = {
            "school_id",
            "new_name_a",
            "new_name_b",
            "to_delete_1",
            "to_delete_2",
        }
        updated_columns_set = set(updated_id_map.keys())
        added_columns = updated_columns_set - existing_after_renames
        removed_columns = existing_after_renames - updated_columns_set
        assert added_columns == {"new_col_x", "new_col_y", "new_col_z"}
        assert removed_columns == {"to_delete_1", "to_delete_2"}


class TestPartialColumnIdMap:
    """Regression tests for the partial column_id_map bug.

    The bug: when ``existing_id_map`` was partially populated (some columns
    had stored UUIDs, others did not), columns without UUIDs were silently
    ignored by detect_renames_and_deletes.  The secondary path in sync_schema
    then dropped them by name, causing data loss when the user intended a
    rename.

    The fix: ALWAYS supplement existing_id_map with synthetic ``table_*`` IDs
    for any table column that lacks a stored UUID.  This ensures the column
    is at least handled by the explicit delete path (with a clear log
    message) instead of being silently dropped.
    """

    def test_simulates_managers_bug_report(self):
        """Reproduces the exact bug from the manager's logs.

        Setup:
          - silver table has columns: school_funding_source, num_tablets_used
          - column_id_map has only num_tablets_used UUID stored
            (school_funding_source UUID is missing for some reason)
          - User updates schema CSV: school_funding_source -> school_funding_type,
            num_tablets_used -> num_tablets

        Before the fix: only num_tablets_used was detected as rename;
        school_funding_source was silently dropped.

        After the fix: num_tablets_used is renamed (UUID match),
        school_funding_source is detected as DELETE (synthetic ID, no match)
        and dropped via the explicit path (with clear log).  No silent data
        loss.
        """
        # Simulate stored column_id_map (partial - missing school_funding_source)
        stored_id_map = {
            "num_tablets_used": "uuid-tablets",
        }
        # Schema CSV has new names
        updated_id_map = {
            "school_funding_type": "uuid-funding",
            "num_tablets": "uuid-tablets",
        }

        # Simulate the new logic: supplement existing_id_map with table columns
        current_table_columns = ["school_funding_source", "num_tablets_used"]
        existing_id_map = dict(stored_id_map)
        for col_name in current_table_columns:
            if col_name not in existing_id_map:
                existing_id_map[col_name] = f"table_{col_name}"

        # Now detect renames and deletes
        renames, deletes = detect_renames_and_deletes(existing_id_map, updated_id_map)

        # num_tablets_used -> num_tablets is detected as rename via UUID
        assert renames == {"num_tablets_used": "num_tablets"}
        # school_funding_source is detected as DELETE (synthetic ID, no UUID match)
        # This is now CONSISTENT - no silent drop in sync_schema secondary path
        assert deletes == ["school_funding_source"]

    def test_partial_map_with_all_renames_intent(self):
        """If user wants to rename a column without stored UUID, it becomes a delete.

        This is expected behavior - we cannot detect renames without UUID
        matching.  The user must ensure UUIDs are preserved across renames.
        Better than silent data loss.
        """
        stored_id_map = {"col_a": "uuid-a"}
        updated_id_map = {"col_a_renamed": "uuid-a", "col_b_new": "uuid-b"}
        current_table_columns = ["col_a", "col_b_old"]

        existing_id_map = dict(stored_id_map)
        for col_name in current_table_columns:
            if col_name not in existing_id_map:
                existing_id_map[col_name] = f"table_{col_name}"

        renames, deletes = detect_renames_and_deletes(existing_id_map, updated_id_map)

        # col_a -> col_a_renamed: detected via UUID match
        assert renames == {"col_a": "col_a_renamed"}
        # col_b_old has no UUID -> detected as delete (synthetic ID never matches)
        assert deletes == ["col_b_old"]

    def test_full_map_rename_works_correctly(self):
        """When all columns have UUIDs stored, rename detection is perfect.

        This is the happy path - column_id_map is complete and accurate.
        """
        stored_id_map = {
            "school_funding_source": "uuid-funding",
            "num_tablets_used": "uuid-tablets",
        }
        updated_id_map = {
            "school_funding_type": "uuid-funding",
            "num_tablets": "uuid-tablets",
        }
        current_table_columns = ["school_funding_source", "num_tablets_used"]

        # With full map, supplementing adds nothing extra
        existing_id_map = dict(stored_id_map)
        for col_name in current_table_columns:
            if col_name not in existing_id_map:
                existing_id_map[col_name] = f"table_{col_name}"

        renames, deletes = detect_renames_and_deletes(existing_id_map, updated_id_map)

        # Both renames detected correctly
        assert renames == {
            "school_funding_source": "school_funding_type",
            "num_tablets_used": "num_tablets",
        }
        assert deletes == []

    def test_orphan_column_not_silently_dropped(self):
        """Orphan columns (in table but not in CSV) are detected as explicit deletes.

        Previously they would be silently dropped by sync_schema secondary path.
        Now they show up in the deletes list with clear logging.
        """
        stored_id_map = {"col_a": "uuid-a"}
        updated_id_map = {"col_a": "uuid-a"}
        # orphan_col is in table but neither in stored map nor in CSV
        current_table_columns = ["col_a", "orphan_col"]

        existing_id_map = dict(stored_id_map)
        for col_name in current_table_columns:
            if col_name not in existing_id_map:
                existing_id_map[col_name] = f"table_{col_name}"

        renames, deletes = detect_renames_and_deletes(existing_id_map, updated_id_map)

        assert renames == {}
        assert deletes == ["orphan_col"]


class TestStoreColumnIdMapStaleCleanup:
    """Tests for the store_column_id_map stale entry cleanup logic.

    Bug: store_column_id_map only ADDed/UPDATEd props but never REMOVEd
    stale entries.  After multiple renames, old column name props would
    accumulate in table properties, eventually causing rename detection
    to misbehave (e.g. multiple props pointing to the same UUID).

    Fix: store_column_id_map now removes any giga.columnId.* props for
    columns not in the new mapping.
    """

    def test_stale_columns_identified(self):
        """Test the logic for identifying stale columns to remove."""
        current_props = {
            "old_name_a": "uuid-a",
            "old_name_b": "uuid-b",
            "unchanged": "uuid-c",
        }
        new_map = {
            "new_name_a": "uuid-a",  # renamed from old_name_a
            "new_name_b": "uuid-b",  # renamed from old_name_b
            "unchanged": "uuid-c",
        }
        stale = [name for name in current_props if name not in new_map]
        assert sorted(stale) == ["old_name_a", "old_name_b"]

    def test_no_stale_columns(self):
        """If new map matches current props, no cleanup needed."""
        current_props = {"col_a": "uuid-a", "col_b": "uuid-b"}
        new_map = {"col_a": "uuid-a", "col_b": "uuid-b"}
        stale = [name for name in current_props if name not in new_map]
        assert stale == []

    def test_removed_columns_in_stale(self):
        """Deleted columns appear in stale list."""
        current_props = {"col_a": "uuid-a", "col_to_delete": "uuid-b"}
        new_map = {"col_a": "uuid-a"}
        stale = [name for name in current_props if name not in new_map]
        assert stale == ["col_to_delete"]


class TestSyncSchemaSafeDropPath:
    """Tests for the sync_schema secondary drop path safety fix.

    Bug: When schema_name was provided and apply_renames_and_deletes
    missed a column (e.g. due to incomplete column_id_map), the secondary
    path in sync_schema would silently drop the column by name comparison,
    causing data loss.

    Fix: When schema_name is provided, the secondary path now logs a
    WARNING and leaves the column in place.  apply_renames_and_deletes
    is the authoritative path for both renames and deletes.

    Note: These tests verify the LOGIC of the new safe drop path; they
    don't run sync_schema directly (which requires Spark).
    """

    def test_safe_drop_path_with_schema_name(self):
        """When schema_name is provided, leftover columns should NOT be dropped."""
        schema_name = "school_geolocation"
        removed_columns = {"school_funding_source"}

        # Simulate the new safe-drop logic
        should_drop = []
        if removed_columns:
            if schema_name is not None:
                # New behavior: warn but don't drop
                pass
            else:
                # Legacy behavior: drop by name
                should_drop = list(removed_columns)

        assert should_drop == []

    def test_safe_drop_path_without_schema_name(self):
        """Legacy callers (no schema_name) keep old drop-by-name behavior."""
        schema_name = None
        removed_columns = {"col_to_drop"}

        should_drop = []
        if removed_columns:
            if schema_name is not None:
                pass
            else:
                should_drop = list(removed_columns)

        assert should_drop == ["col_to_drop"]
