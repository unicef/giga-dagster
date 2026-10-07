# Incremental Model Scripts

Scripts executed via Dagster using incremental patterns. Rather than re-scanning full history on
each run, these scripts process only new records — making them efficient for large, fast-growing
tables. Every asset here is in the `incremental` Dagster group, which runs **hourly** (at :15,
`dagster/src/schedule/analytics_tables.py`). `all_ping_daily` runs hourly too but only ever
appends complete past days (`< CURRENT_DATE`), so repeated runs within a day are no-ops.

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

### Delta-combine MERGE (aggregates over the measurement table)

`all_gigameter_school_daily_troubleshooting` (one row per school+country+UTC date) and
`all_gigameter_school_measurement_stats` (one row per school) are aggregates of
`all_gigameter_measurement_data` in which **every column can be combined exactly** (counts and
sums add, min/max compare, `min_by`/`max_by` follow the numeric version, arrays union, count maps
add per key). Each stores `max_measurement_id`; the `update/` script aggregates only rows with
`measurement_id > MAX(max_measurement_id)` of its own table to the target grain and `MERGE`s them
in, combining old and new values. A run never re-reads history, late/back-dated measurements
update the right existing row, and the watermark commits atomically with the data. No batch cap —
capping rows before a `GROUP BY` would split groups across runs. Merge keys that can be NULL
(unmatched MLab rows) are matched with `IS NOT DISTINCT FROM`.

### Accumulator + cheap full rebuild (`all_gigameter_registered_schools`)

`all_gigameter_registered_schools` is a snapshot of every school in `all_school_master` whose
columns change for reasons other than new measurements (master attributes, registrations,
lookups, `CURRENT_DATE`-relative status). Only the lifetime measurement aggregates grow forever,
so those live in the incremental accumulator `all_gigameter_school_measurement_stats` (above); the
final table is then rebuilt every hour by a cheap join — `create/` once, `update/` with
`CREATE OR REPLACE TABLE`, which swaps the new version in atomically (no window where the table
is missing, Delta history kept). The two scripts' SELECTs must stay identical. The map columns
are the exact top 10 of the stored counts, most-frequent first.

### Chunked bootstrap

`create/all_ping_hourly_incremental.sql` (the one-time bootstrap) is also non-standard: unlike
every other `create/` script here, which is a single unbounded statement, it's a sequence of
one `CREATE TABLE` plus several `INSERT`s, each bounded to a disjoint, hour-aligned local-time
window — a fix for a production `EXCEEDED_LOCAL_MEMORY_LIMIT` failure when the full unbounded
history was aggregated in one `GROUP BY`. See the header comment in that file for the chunk
sizing and stop-point reasoning.

---

## Scripts

| Script | Cadence | Creates Table | Purpose | Depends On |
|---|---|---|---|---|
| `all_gmeter_only_measurements.sql` | Hourly | `default.all_gmeter_only_measurements` | GigaMeter app raw measurements — incremental Step 1 | `gigameter_production_db` |
| `all_mlab_only_measurements.sql` | Hourly | `default.all_mlab_only_measurements` | MLab network test measurements — incremental Step 2 | `gigameter_production_db` |
| `all_gigameter_valid_test_checker.sql` | Hourly | `default.all_gigameter_valid_test_checker` | Per-measurement quality validation — incremental Step 3 | Steps 1 & 2 incremental |
| `all_gigameter_measurement_data.sql` | Hourly | `default.all_gigameter_measurement_data` | Consolidated validated measurements — incremental Step 4 | Steps 1, 2, & 3 incremental |
| `all_ping_hourly.sql` | Hourly | `default.all_ping_hourly` | Hourly ping/uptime aggregation per device-school (`MERGE`, not `INSERT` — see above) | `gigameter_production_db.connectivity_ping_checks` |
| `all_ping_daily.sql` | Hourly (appends complete past days only) | `default.all_ping_daily` | Daily ping aggregation | `all_ping_hourly` |
| `all_gigameter_school_daily_troubleshooting.sql` | Hourly | `default.all_gigameter_school_daily_troubleshooting` | School+day summary for Superset T0 troubleshooting (`MERGE`, delta-combine) | Step 4 |
| `all_gigameter_school_measurement_stats.sql` | Hourly | `default.all_gigameter_school_measurement_stats` | **Internal** lifetime measurement aggregates per school (`MERGE`, delta-combine) — not for dashboards | Step 4 |
| `all_gigameter_registered_schools.sql` | Hourly | `default.all_gigameter_registered_schools` | School-level registration & activity summary (full rebuild via `CREATE OR REPLACE`) | `all_gigameter_school_measurement_stats`, `all_school_master` (daily), `dailycheckapp_school`, `lstringer.*` lookups |

Script filenames and their `@asset` function names dropped the `_incremental` suffix as part of
the dev→prod cutover (mirrors `unicef/giga-data-analytics#25`) — each now writes directly to the
same table name the retired `daily/` script used to own, since downstream consumers (other daily
assets, Superset dashboards) already read that plain name. The `key_prefix=["incremental"]` on
each `@asset` still keeps the Dagster `AssetKey` distinct from any same-named daily asset.

Run each chain in order; each step depends on the previous within its own chain. The ping chain
is independent of the measurement chain (Steps 1–4) — neither depends on the other.

---

## Execution Order

```
Measurement chain (hourly):
Step 1:  all_gmeter_only_measurements
Step 2:  all_mlab_only_measurements
Step 3:  all_gigameter_valid_test_checker
Step 4:  all_gigameter_measurement_data
Step 5a: all_gigameter_school_daily_troubleshooting   ← Step 4
Step 5b: all_gigameter_school_measurement_stats       ← Step 4
Step 6:  all_gigameter_registered_schools             ← Step 5b (+ daily all_school_master)

Ping chain (hourly):
Step 1:  all_ping_hourly
Step 2:  all_ping_daily    (appends only complete past days, so hourly re-runs are no-ops)
```

---

## Relationship to Daily Scripts

These scripts are now the sole producers of Steps 1–4 and the two ping tables — the daily
versions and `@asset` functions in [`../daily/`](../daily/README.md) that used to create
`all_gmeter_only_measurements`, `all_mlab_only_measurements`, `all_gigameter_valid_test_checker`,
`all_gigameter_measurement_data`, `all_ping_hourly`, and `all_ping_daily` have been retired. Every
other script in `../daily/` that reads one of these tables is unaffected — it keeps reading the
same table name via an updated `deps=[AssetKey(["incremental", ...])]`, now populated
hourly/daily by the scripts here instead of being dropped and recreated once a day.

`all_gigameter_school_daily_troubleshooting` and `all_gigameter_registered_schools` have also
moved here from `../daily/` (same table names; the daily scripts and assets are retired). The
daily `mng_gigameter_qos_registered` now depends on
`AssetKey(["incremental", "all_gigameter_registered_schools"])`.

Next up for conversion: `mng_gigameter_qos_measurements` and `bra_nicbr_daily` (daily aggregations), then `all_gigameter_registered_devices` — a full device-state snapshot, which can follow the accumulator + rebuild split used for `all_gigameter_registered_schools`.
