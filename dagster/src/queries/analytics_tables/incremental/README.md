# Incremental Model Scripts

Scripts executed on an **hourly cadence** via Dagster using an incremental insert pattern. Rather than recreating the full table on each run, these scripts append only new records — making them efficient for large, fast-growing tables.

Scripts are sourced from the **Incremental Models** NocoDB table (`ma6e34qsc3t11ah`).

---

## How Incremental Models Work

Each script:
1. Reads from the production source tables
2. Filters to only records newer than the latest timestamp already in the target table (with a 1-hour catch-up cap to handle backlog after failures)
3. Deduplicates by `measurement_id` to prevent double-inserts
4. **Inserts** new rows — does not drop or recreate the table

This pattern is fault-tolerant: if an hourly run fails, the next run automatically catches up the missed window.

---

## Scripts

| Script | Creates Table | Purpose | Depends On |
|---|---|---|---|
| `all_gmeter_only_measurements_incremental.sql` | `default.all_gmeter_only_measurements_incremental` | GigaMeter app raw measurements — incremental Step 1 | `gigameter_production_db` |
| `all_mlab_only_measurements_incremental.sql` | `default.all_mlab_only_measurements_incremental` | MLab network test measurements — incremental Step 2 | `gigameter_production_db` |
| `all_gigameter_valid_test_checker_incremental.sql` | `default.all_gigameter_valid_test_checker_incremental` | Per-measurement quality validation — incremental Step 3 | Steps 1 & 2 incremental |
| `all_gigameter_measurement_data_incremental.sql` | `default.all_gigameter_measurement_data_incremental` | Consolidated validated measurements — incremental Step 4 | Steps 1, 2, & 3 incremental |

Run in order (Step 1 → 2 → 3 → 4); each step depends on the previous.

---

## Execution Order

```
Step 1:  all_gmeter_only_measurements_incremental
Step 2:  all_mlab_only_measurements_incremental
Step 3:  all_gigameter_valid_test_checker_incremental
Step 4:  all_gigameter_measurement_data_incremental
```

---

## Relationship to Daily Scripts

These scripts create **separate tables** (with `_incremental` suffix) that run in parallel with the existing daily equivalents in [`../daily/`](../daily/README.md) while validation is ongoing. Once confirmed working in production, the daily versions of Steps 1–4 will be retired.

A second iteration will extend incremental models to cover registration and regional scripts (`all_gigameter_registered_schools`, `all_gigameter_registered_devices`, `bra_nicbr_*`, `mng_gigameter_*`).
