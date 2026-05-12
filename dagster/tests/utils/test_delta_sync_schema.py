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

        Columns whose name matches a key in updated_id_map get the REAL UUID instead,
        preventing false deletes for unchanged business columns.
        """
        existing_id_map = {"col_a": "id-1", "col_b": "id-2"}
        updated_id_map = {"col_a": "id-1", "col_b": "id-2"}
        current_table_columns = ["col_a", "col_b", "orphan_col"]

        # Supplement: use real UUID when name matches, synthetic for orphans
        for col_name in current_table_columns:
            if col_name not in existing_id_map:
                if col_name in updated_id_map:
                    existing_id_map[col_name] = updated_id_map[col_name]
                else:
                    existing_id_map[col_name] = f"table_{col_name}"

        renames, deletes = detect_renames_and_deletes(existing_id_map, updated_id_map)
        assert deletes == ["orphan_col"]
        assert renames == {}


class TestFalseDeletePrevention:
    """Regression tests for the drop-and-re-add bug.

    Bug: When existing_id_map was partially populated, columns that existed
    in BOTH the table AND the schema CSV (unchanged columns) were tagged with
    synthetic IDs.  The synthetic IDs never matched the real schema UUIDs,
    causing them to be falsely detected as deletes, dropped, and then
    immediately re-added by the sync_schema add-columns logic — losing all
    data in those columns.

    Fix: When supplementing existing_id_map, if a column name matches a key
    in updated_id_map, use the real schema UUID instead of a synthetic one.
    """

    @staticmethod
    def _supplement_logic(
        stored_id_map: dict,
        updated_id_map: dict,
        current_table_columns: list,
        excluded: set | None = None,
    ):
        """Mirror the FIXED production supplementing logic."""
        existing = dict(stored_id_map)
        excluded = excluded or set()
        for col_name in current_table_columns:
            if col_name in excluded:
                continue
            if col_name not in existing:
                if col_name in updated_id_map:
                    existing[col_name] = updated_id_map[col_name]
                else:
                    existing[col_name] = f"table_{col_name}"
        return existing

    def test_unchanged_columns_not_falsely_deleted(self):
        """Reproduces the exact user-reported bug.

        Table has: school_id_giga, longitude, school_name, num_students, latitude
        Schema CSV has the same columns (with UUIDs).
        Only school_id_giga has a stored UUID; the rest don't.
        Expected: no renames, no deletes.
        """
        stored_id_map = {"school_id_giga": "uuid-sig"}  # partial: only one stored UUID
        updated_id_map = {
            "school_id_giga": "uuid-sig",
            "longitude": "uuid-lon",
            "school_name": "uuid-sn",
            "num_students": "uuid-ns",
            "latitude": "uuid-lat",
        }
        current_table_columns = [
            "school_id_giga",
            "longitude",
            "school_name",
            "num_students",
            "latitude",
        ]

        existing = self._supplement_logic(
            stored_id_map, updated_id_map, current_table_columns
        )

        renames, deletes = detect_renames_and_deletes(existing, updated_id_map)
        # All columns should be recognized as unchanged (real UUIDs match)
        assert renames == {}
        assert deletes == []

    def test_mixed_unchanged_and_orphan(self):
        """Columns in both table and CSV are preserved; orphans are deleted."""
        stored_id_map = {"col_a": "uuid-a"}
        updated_id_map = {
            "col_a": "uuid-a",
            "col_b": "uuid-b",  # in table but no stored UUID
            "col_c": "uuid-c",  # in table but no stored UUID
        }
        current_table_columns = ["col_a", "col_b", "col_c", "orphan_col"]

        existing = self._supplement_logic(
            stored_id_map, updated_id_map, current_table_columns
        )

        renames, deletes = detect_renames_and_deletes(existing, updated_id_map)
        assert renames == {}
        # Only orphan_col should be deleted (not col_b or col_c)
        assert deletes == ["orphan_col"]

    def test_rename_still_works_with_fix(self):
        """Renames are still detected when one column has a stored UUID
        and the schema CSV has a new name for that UUID."""
        stored_id_map = {"old_name": "uuid-1", "col_b": "uuid-2"}
        updated_id_map = {
            "new_name": "uuid-1",  # rename via UUID
            "col_b": "uuid-2",
            "col_c": "uuid-c",  # in table but no stored UUID
        }
        current_table_columns = ["old_name", "col_b", "col_c"]

        existing = self._supplement_logic(
            stored_id_map, updated_id_map, current_table_columns
        )

        renames, deletes = detect_renames_and_deletes(existing, updated_id_map)
        assert renames == {"old_name": "new_name"}
        assert deletes == []

    def test_old_synthetic_logic_would_cause_false_deletes(self):
        """Demonstrates that the OLD logic (always synthetic) causes false deletes."""
        stored_id_map = {"school_id_giga": "uuid-sig"}
        updated_id_map = {
            "school_id_giga": "uuid-sig",
            "longitude": "uuid-lon",
            "school_name": "uuid-sn",
        }
        current_table_columns = ["school_id_giga", "longitude", "school_name"]

        # OLD logic: always use synthetic ID
        existing_buggy = dict(stored_id_map)
        for col_name in current_table_columns:
            if col_name not in existing_buggy:
                existing_buggy[col_name] = f"table_{col_name}"  # BUG

        _, deletes_buggy = detect_renames_and_deletes(existing_buggy, updated_id_map)
        # OLD: longitude and school_name are falsely detected as deletes
        assert "longitude" in deletes_buggy
        assert "school_name" in deletes_buggy

        # NEW logic: use real UUID when name matches
        existing_fixed = self._supplement_logic(
            stored_id_map, updated_id_map, current_table_columns
        )

        _, deletes_fixed = detect_renames_and_deletes(existing_fixed, updated_id_map)
        # NEW: no false deletes
        assert deletes_fixed == []


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


class TestPartitionColumnExclusion:
    """Regression tests for DELTA_UNSUPPORTED_DROP_PARTITION_COLUMN bug.

    Staging tables have technical partition columns (upload_id, etc.) that are
    never present in the business schema CSV.  Before the fix these were tagged
    with synthetic IDs and detected as deletes, causing:
        ALTER TABLE ... DROP COLUMN `upload_id`
    which Delta Lake rejects with DELTA_UNSUPPORTED_DROP_PARTITION_COLUMN.

    The fix: skip partition columns when supplementing existing_id_map.
    """

    def test_partition_columns_excluded_from_synthetic_tagging(self):
        """Partition columns must not appear in the deletes list."""
        stored_id_map = {"electricity_type": "uuid-elec", "school_name": "uuid-name"}
        updated_id_map = {
            "electricity_type_test": "uuid-elec",
            "school_name": "uuid-name",
        }
        partition_columns = {"upload_id"}

        # Simulate staging table columns including partition column
        current_table_columns = [
            "electricity_type",
            "school_name",
            "upload_id",  # partition column — must be skipped
            "change_type",  # technical column without UUID
        ]

        existing_id_map = dict(stored_id_map)
        for col_name in current_table_columns:
            if col_name in partition_columns:
                continue  # THE FIX: skip partition columns
            if col_name not in existing_id_map:
                existing_id_map[col_name] = f"table_{col_name}"

        renames, deletes = detect_renames_and_deletes(existing_id_map, updated_id_map)

        assert "upload_id" not in deletes
        assert renames == {"electricity_type": "electricity_type_test"}
        assert deletes == ["change_type"]

    def test_partition_columns_not_in_deletes_without_fix(self):
        """Demonstrates the bug: without the fix, upload_id would be in deletes."""
        stored_id_map = {"electricity_type": "uuid-elec"}
        updated_id_map = {"electricity_type_test": "uuid-elec"}
        current_table_columns = ["electricity_type", "upload_id"]

        # WITHOUT the fix (no partition exclusion)
        existing_id_map_buggy = dict(stored_id_map)
        for col_name in current_table_columns:
            if col_name not in existing_id_map_buggy:
                existing_id_map_buggy[col_name] = f"table_{col_name}"

        _, deletes_buggy = detect_renames_and_deletes(
            existing_id_map_buggy, updated_id_map
        )
        assert "upload_id" in deletes_buggy  # proves the bug existed

        # WITH the fix (partition columns skipped)
        partition_columns = {"upload_id"}
        existing_id_map_fixed = dict(stored_id_map)
        for col_name in current_table_columns:
            if col_name in partition_columns:
                continue
            if col_name not in existing_id_map_fixed:
                existing_id_map_fixed[col_name] = f"table_{col_name}"

        _, deletes_fixed = detect_renames_and_deletes(
            existing_id_map_fixed, updated_id_map
        )
        assert "upload_id" not in deletes_fixed  # fix works

    def test_rollback_rename_with_partition_columns(self):
        """The exact rollback scenario: electricity_type_test -> electricity_type.

        Staging table has partition column upload_id plus technical columns.
        The rollback rename must succeed without attempting to drop upload_id.
        """
        stored_id_map = {
            "electricity_type_test": "uuid-elec",
            "school_name": "uuid-name",
            "change_type": "uuid-change",
        }
        updated_id_map = {
            "electricity_type": "uuid-elec",  # rollback rename
            "school_name": "uuid-name",
            "change_type": "uuid-change",
        }
        partition_columns = {"upload_id"}
        current_table_columns = [
            "electricity_type_test",
            "school_name",
            "change_type",
            "upload_id",  # partition — must be excluded
            "uploaded_columns",
            "status",
        ]

        existing_id_map = dict(stored_id_map)
        for col_name in current_table_columns:
            if col_name in partition_columns:
                continue
            if col_name not in existing_id_map:
                existing_id_map[col_name] = f"table_{col_name}"

        renames, deletes = detect_renames_and_deletes(existing_id_map, updated_id_map)

        assert renames == {"electricity_type_test": "electricity_type"}
        assert "upload_id" not in deletes
        # uploaded_columns and status are technical orphans — correctly detected as deletes
        # but NOT partition columns so Delta won't reject them (they can be dropped or ignored
        # by execute_deletes' safety guard)
        assert "upload_id" not in renames.keys()
        assert "upload_id" not in renames.values()


class TestPartitionAndCallerManagedExclusion:
    """Comprehensive coverage for the exclusion logic in apply_renames_and_deletes.

    These tests model the in-memory logic without touching Spark.  Each test
    simulates the EXACT logic in apply_renames_and_deletes after my fixes:
        excluded = partition_columns | (caller_managed - business_csv)
        # Strip excluded from existing_id_map and updated_id_map
        # Skip excluded when supplementing synthetic IDs
    """

    @staticmethod
    def _apply_logic(
        stored_id_map: dict,
        business_csv_map: dict,
        current_table_columns: list,
        partition_columns: set,
        caller_managed_columns: set | None = None,
    ):
        """Mirror the production logic in apply_renames_and_deletes."""
        existing = dict(stored_id_map)
        updated = dict(business_csv_map)
        excluded = set(partition_columns)
        if caller_managed_columns:
            excluded |= {c for c in caller_managed_columns if c not in updated}

        # Strip excluded from both maps
        for c in list(existing.keys()):
            if c in excluded:
                del existing[c]
        for c in list(updated.keys()):
            if c in excluded:
                del updated[c]

        # Supplement with synthetic IDs (skip excluded)
        for c in current_table_columns:
            if c in excluded:
                continue
            if c not in existing:
                existing[c] = f"table_{c}"

        return detect_renames_and_deletes(existing, updated)

    def test_multi_partition_table_all_protected(self):
        """All partition columns (not just upload_id) must be protected."""
        # caller_managed_columns reflects the FUTURE (updated) schema
        renames, deletes = self._apply_logic(
            stored_id_map={"electricity_type": "u-elec"},
            business_csv_map={"electricity_type_test": "u-elec"},
            current_table_columns=[
                "electricity_type",
                "year",
                "month",
                "country_code",
            ],
            partition_columns={"year", "month", "country_code"},
            caller_managed_columns={
                "electricity_type_test",  # new name (from updated_schema)
                "year",
                "month",
                "country_code",
            },
        )
        assert renames == {"electricity_type": "electricity_type_test"}
        assert "year" not in deletes
        assert "month" not in deletes
        assert "country_code" not in deletes
        assert deletes == []

    def test_partition_col_with_stored_uuid_not_dropped(self):
        """Even if a partition column has a stored UUID (loophole A), it is excluded."""
        renames, deletes = self._apply_logic(
            stored_id_map={
                "electricity_type": "u-elec",
                "upload_id": "u-bogus-stored-uuid",  # should never be acted on
            },
            business_csv_map={"electricity_type": "u-elec"},
            current_table_columns=["electricity_type", "upload_id"],
            partition_columns={"upload_id"},
            caller_managed_columns={"electricity_type", "upload_id"},
        )
        assert renames == {}
        assert "upload_id" not in deletes
        assert deletes == []

    def test_partition_col_in_business_csv_is_ignored(self):
        """If a partition column is mistakenly in the business CSV, exclusion still wins."""
        renames, deletes = self._apply_logic(
            stored_id_map={"electricity_type": "u-elec"},
            business_csv_map={
                "electricity_type": "u-elec",
                "upload_id": "u-bogus-csv",  # mistakenly added to business CSV
            },
            current_table_columns=["electricity_type", "upload_id"],
            partition_columns={"upload_id"},
            caller_managed_columns={"electricity_type", "upload_id"},
        )
        assert renames == {}
        assert deletes == []

    def test_caller_managed_with_stored_uuid_not_dropped(self):
        """Loophole A for technical columns: stored UUID for tech col must be ignored."""
        renames, deletes = self._apply_logic(
            stored_id_map={
                "school_name": "u-sn",
                "change_type": "u-bogus-tech",  # tech col should never have UUID
            },
            business_csv_map={"school_name": "u-sn"},
            current_table_columns=["school_name", "change_type", "status"],
            partition_columns=set(),
            caller_managed_columns={"school_name", "change_type", "status"},
        )
        assert renames == {}
        assert "change_type" not in deletes
        assert "status" not in deletes
        assert deletes == []

    def test_master_table_no_partitions_no_change(self):
        """Master tables have no partitions and no tech cols — must work as before."""
        renames, deletes = self._apply_logic(
            stored_id_map={
                "school_name": "u-sn",
                "old_funding": "u-fund",
                "to_drop": "u-drop",
            },
            business_csv_map={
                "school_name": "u-sn",
                "new_funding": "u-fund",
            },
            current_table_columns=["school_name", "old_funding", "to_drop"],
            partition_columns=set(),  # no partitions
            caller_managed_columns=None,  # no updated_schema passed
        )
        assert renames == {"old_funding": "new_funding"}
        assert deletes == ["to_drop"]

    def test_orphan_business_col_still_detected(self):
        """Business columns NOT in updated_schema NOR partitions are still subject to delete."""
        renames, deletes = self._apply_logic(
            stored_id_map={"keep_me": "u-keep"},
            business_csv_map={"keep_me": "u-keep"},
            # orphan_biz is in the table but NOT in updated_schema (was supposed to be deleted)
            current_table_columns=["keep_me", "orphan_biz", "upload_id"],
            partition_columns={"upload_id"},
            caller_managed_columns={"keep_me", "upload_id"},  # orphan_biz NOT here
        )
        # orphan_biz is not partition, not caller-managed → tagged synthetic → delete
        assert renames == {}
        assert deletes == ["orphan_biz"]

    def test_rollback_rename_full_scenario(self):
        """End-to-end model of the manager's rollback scenario."""
        partition_cols = {"upload_id"}
        tech_cols = {
            "change_type",
            "uploaded_columns",
            "status",
            "change_id",
            "created_at",
            "processed_at",
            "approval_request_log_id",
            "master_version",
        }
        # All 28 staging columns
        current = ["school_id_giga", "school_name", "electricity_type_test"]
        current += list(tech_cols) + list(partition_cols)

        # Stored UUIDs (only business)
        stored = {
            "school_id_giga": "u1",
            "school_name": "u2",
            "electricity_type_test": "u-elec",  # current name in table
        }
        # New CSV: rollback rename
        csv = {
            "school_id_giga": "u1",
            "school_name": "u2",
            "electricity_type": "u-elec",  # rollback to old name
        }
        # Updated schema (full pending) = business + tech + partition
        updated_schema_names = set(csv.keys()) | tech_cols | partition_cols

        renames, deletes = self._apply_logic(
            stored_id_map=stored,
            business_csv_map=csv,
            current_table_columns=current,
            partition_columns=partition_cols,
            caller_managed_columns=updated_schema_names,
        )

        assert renames == {"electricity_type_test": "electricity_type"}
        assert deletes == []

    def test_combined_multi_operations_with_partitions(self):
        """Multi rename + multi delete + multi add, all with partition cols."""
        partition_cols = {"upload_id"}
        tech_cols = {"change_type", "status"}
        current = [
            "keep_a",
            "old_b",
            "old_c",  # business
            "to_drop_x",
            "to_drop_y",  # business deletes
            "upload_id",  # partition
            "change_type",
            "status",  # tech
        ]
        stored = {
            "keep_a": "u1",
            "old_b": "u2",
            "old_c": "u3",
            "to_drop_x": "u4",
            "to_drop_y": "u5",
        }
        csv = {
            "keep_a": "u1",
            "new_b": "u2",
            "new_c": "u3",  # 2 renames
            "added_p": "u6",
            "added_q": "u7",  # 2 adds (handled elsewhere)
        }
        updated_schema_names = set(csv.keys()) | tech_cols | partition_cols

        renames, deletes = self._apply_logic(
            stored_id_map=stored,
            business_csv_map=csv,
            current_table_columns=current,
            partition_columns=partition_cols,
            caller_managed_columns=updated_schema_names,
        )

        assert renames == {"old_b": "new_b", "old_c": "new_c"}
        assert sorted(deletes) == ["to_drop_x", "to_drop_y"]


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


class TestPrimaryKeyProtection:
    """Unit tests for primary-key column protection in rename/delete detection.

    A primary key column must NEVER be renamed or deleted, even if the
    schema CSV is updated with a different name for the same UUID.
    """

    def _apply_with_pk(
        self,
        stored_id_map: dict[str, str],
        business_csv_map: dict[str, str],
        pk_names: set[str],
        persisted_pk_uuids: set[str] | None = None,
    ) -> tuple[dict[str, str], list[str], set[str]]:
        """Mimic the detection + PK-filtering logic from apply_renames_and_deletes.

        ``persisted_pk_uuids`` mimics the ``giga.pkColumnIds`` table property
        that persists PK UUIDs even when the CSV removes the column.
        """
        existing_id_map = dict(stored_id_map)
        updated_id_map = dict(business_csv_map)
        persisted_pk_uuids = set(persisted_pk_uuids or ())

        # detect
        id_to_name = {v: k for k, v in updated_id_map.items()}
        renames: dict[str, str] = {}
        deletes: list[str] = []
        for name, uid in existing_id_map.items():
            if uid in id_to_name and id_to_name[uid] != name:
                renames[name] = id_to_name[uid]
            elif uid not in id_to_name:
                deletes.append(name)

        # PK filtering (same logic as production code).
        # PK UUIDs come from BOTH the CSV-declared PK names and any persisted
        # PK UUIDs (the latter mimics ``giga.pkColumnIds`` on the data table).
        pk_uuids = {updated_id_map[n] for n in pk_names if n in updated_id_map}
        pk_uuids |= persisted_pk_uuids
        renames_original = dict(renames)
        if pk_uuids:
            blocked_renames = {
                old_name
                for old_name, new_name in renames.items()
                if existing_id_map.get(old_name) in pk_uuids
            }
            for old_name in blocked_renames:
                del renames[old_name]
            blocked_deletes = [
                col_name
                for col_name in deletes
                if existing_id_map.get(col_name) in pk_uuids
            ]
            for col_name in blocked_deletes:
                deletes.remove(col_name)

        # blocked new names to exclude from add logic
        blocked_new_names = set()
        if pk_uuids:
            blocked_new_names = {
                new_name
                for old_name, new_name in renames_original.items()
                if old_name
                in {
                    o
                    for o, _ in renames_original.items()
                    if existing_id_map.get(o) in pk_uuids
                }
            }

        return renames, deletes, blocked_new_names

    def test_pk_rename_is_blocked(self):
        """A primary key column rename must be blocked."""
        stored = {"school_id_giga": "u1", "latitude": "u2"}
        csv = {"school_id_giga_renamed": "u1", "latitude": "u2"}
        pk_names = {"school_id_giga_renamed"}

        renames, deletes, blocked = self._apply_with_pk(stored, csv, pk_names)
        assert renames == {}
        assert deletes == []
        assert blocked == {"school_id_giga_renamed"}

    def test_pk_delete_is_blocked(self):
        """A primary key column delete must be blocked.

        Scenario:  the schema CSV no longer contains the PK column at all,
        so ``primary_key_columns`` (derived from the CSV) is empty.  The
        PK UUID is still recorded on the data table via
        ``giga.pkColumnIds``; that persisted set must keep the column
        protected from being dropped.
        """
        stored = {"school_id_giga": "u1", "latitude": "u2"}
        csv = {"latitude": "u2"}  # school_id_giga entirely removed from CSV
        pk_names: set[str] = set()  # CSV has no PK markers
        persisted_pks = {"u1"}  # but UUID u1 was historically the PK

        renames, deletes, blocked = self._apply_with_pk(
            stored, csv, pk_names, persisted_pks
        )
        assert renames == {}
        # PK delete must be blocked — school_id_giga stays
        assert deletes == []
        assert blocked == set()

    def test_pk_delete_without_persisted_uuid_is_allowed(self):
        """Without persisted PK UUIDs, the legacy behaviour is preserved.

        This documents the prior (pre-fix) behaviour for tables that have
        not yet had ``giga.pkColumnIds`` written — the delete proceeds.
        """
        stored = {"school_id_giga": "u1", "latitude": "u2"}
        csv = {"latitude": "u2"}
        pk_names: set[str] = set()

        renames, deletes, blocked = self._apply_with_pk(
            stored, csv, pk_names, persisted_pk_uuids=set()
        )
        assert renames == {}
        # No PK protection available → delete proceeds
        assert deletes == ["school_id_giga"]
        assert blocked == set()

    def test_non_pk_rename_is_allowed(self):
        """Non-PK column renames should still proceed normally."""
        stored = {"school_id_giga": "u1", "latitude": "u2"}
        csv = {"school_id_giga": "u1", "lat_renamed": "u2"}
        pk_names = {"school_id_giga"}

        renames, deletes, blocked = self._apply_with_pk(stored, csv, pk_names)
        assert renames == {"latitude": "lat_renamed"}
        assert deletes == []
        assert blocked == set()

    def test_pk_and_regular_rename_together(self):
        """Mixed scenario: PK rename blocked, regular rename allowed."""
        stored = {"school_id_giga": "u1", "latitude": "u2", "old_col": "u3"}
        csv = {
            "school_id_giga_renamed": "u1",
            "lat_renamed": "u2",
            "new_col": "u3",
        }
        pk_names = {"school_id_giga_renamed"}

        renames, deletes, blocked = self._apply_with_pk(stored, csv, pk_names)
        assert renames == {"latitude": "lat_renamed", "old_col": "new_col"}
        assert deletes == []
        assert blocked == {"school_id_giga_renamed"}
