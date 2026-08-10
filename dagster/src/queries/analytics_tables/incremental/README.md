# Incremental Model Scripts

Scripts executed via Dagster using an incremental insert pattern. Rather than recreating the
full table on each run, these scripts append only new records — making them efficient for
large, fast-growing tables. Cadence varies by script — most run **hourly**, but
`all_ping_daily_incremental` runs **daily** (see [Scripts](#scripts) below).

Scripts are maintained in [`unicef/giga-data-analytics`](https://github.com/unicef/giga-data-analytics/tree/main/analytics-tables/incremental) (itself sourced from the **Incremental Models** NocoDB table, `ma6e34qsc3t11ah`, with the exception of the two `all_ping_*_incremental` scripts, which don't yet have a NocoDB source record) and ported here via PR once validated there.

---

## How Incremental Models Work

The four measurement-pipeline scripts (Steps 1–4) are flat, per-row appends:
1. Reads from the production source tables (or another already-incremental table)
2. Filters to `id > (SELECT COALESCE(MAX(measurement_id), 0) FROM <own target table>)` — since
   `measurement_id` is a guaranteed-unique, strictly-increasing integer, this single comparison
   both selects new rows and guarantees no duplicates, with no separate dedup step needed.
   Replaced a 3-part timestamp watermark + `NOT IN` dedup + future-date guard (2026-08-07) —
   the old approach was more expensive (three subqueries against the growing target table
   instead of one sargable comparison) and broke on table recreate/backfill, since it
   bootstrapped from a hardcoded fallback date rather than "everything so far." Filtering out
   source rows with corrupted future timestamps (a known data-quality issue — see
   `gigameter_production_db.public.connectivity_ping_checks` and the raw `measurements` table)
   is now handled separately, downstream in Dagster, not in these scripts.
3. **Inserts** new rows — does not drop or recreate the table

This pattern is fault-tolerant: if a run fails, the next run automatically catches up all
rows with a higher id, however large the gap.

`all_ping_daily_incremental` is also a flat append, but at a different (aggregated
date+school+device) grain with no per-row id column to filter on — it still uses a
date-based watermark (`local_created_date >= MAX(local_created_date)`) plus a composite-key
`NOT EXISTS` anti-join for dedup. Not part of the id-based redesign above.

`all_ping_hourly_incremental` is the one exception: it's a `GROUP BY` aggregation (one row per
school+device+hour), not a flat per-row table, so a plain append can't correct an already-written
bucket if a ping arrives late. It uses `MERGE INTO` instead of `INSERT INTO` — see the comment
block at the top of `update/all_ping_hourly_incremental.sql` for the full reasoning (4-hour grace
period + `connectivity_ids`-based reconciliation).

---

## Scripts

| Script | Cadence | Creates Table | Purpose | Depends On |
|---|---|---|---|---|
| `all_gmeter_only_measurements_incremental.sql` | Hourly | `default.all_gmeter_only_measurements_incremental` | GigaMeter app raw measurements — incremental Step 1 | `gigameter_production_db` |
| `all_mlab_only_measurements_incremental.sql` | Hourly | `default.all_mlab_only_measurements_incremental` | MLab network test measurements — incremental Step 2 | `gigameter_production_db` |
| `all_gigameter_valid_test_checker_incremental.sql` | Hourly | `default.all_gigameter_valid_test_checker_incremental` | Per-measurement quality validation — incremental Step 3 | Steps 1 & 2 incremental |
| `all_gigameter_measurement_data_incremental.sql` | Hourly | `default.all_gigameter_measurement_data_incremental` | Consolidated validated measurements — incremental Step 4 | Steps 1, 2, & 3 incremental |
| `all_ping_hourly_incremental.sql` | Hourly | `default.all_ping_hourly_incremental` | Hourly ping/uptime aggregation per device-school (`MERGE`, not `INSERT` — see above) | `gigameter_production_db.connectivity_ping_checks` |
| `all_ping_daily_incremental.sql` | Daily | `default.all_ping_daily_incremental` | Daily ping aggregation | `all_ping_hourly_incremental` |

Run each chain in order; each step depends on the previous within its own chain. The ping chain
is independent of the measurement chain (Steps 1–4) — neither depends on the other.

---

## Execution Order

```
Measurement chain (hourly):
Step 1:  all_gmeter_only_measurements_incremental
Step 2:  all_mlab_only_measurements_incremental
Step 3:  all_gigameter_valid_test_checker_incremental
Step 4:  all_gigameter_measurement_data_incremental

Ping chain (mixed cadence):
Step 1:  all_ping_hourly_incremental   (hourly)
Step 2:  all_ping_daily_incremental    (daily — must run after that day's hourly buckets have settled)
```

---

## Relationship to Daily Scripts

These scripts create **separate tables** (with `_incremental` suffix) that run in parallel with the existing daily equivalents in [`../daily/`](../daily/README.md) while validation is ongoing. Once confirmed working in production, the daily versions of Steps 1–4 and `all_ping_hourly` / `all_ping_daily` will be retired.

Next up for conversion: `mng_gigameter_qos_measurements` and `bra_nicbr_daily` (daily aggregations, same pattern as the ping chain), then `all_gigameter_registered_schools` and `all_gigameter_registered_devices` — those two are full school/device-state snapshots rather than append-only event aggregations, so they'll need a different conversion approach, not a direct copy of this pattern.
