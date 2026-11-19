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

### Accumulator + MERGE with deleted flag (`all_gigameter_registered_schools`)

`all_gigameter_registered_schools` is one row per school in `all_school_master` whose columns
change for reasons other than new measurements (master attributes, registrations, lookups,
`CURRENT_DATE`-relative status). Only the lifetime measurement aggregates grow forever, so those
live in the incremental accumulator `all_gigameter_school_measurement_stats` (above). The current
state is then computed every hour by a cheap join and applied as a `MERGE`:

- `create/` builds the table once from that SELECT (`CREATE TABLE … AS`).
- `update/` runs a `MERGE` on `(school_id_giga, country)` that inserts new schools and updates
  only rows where some column `IS DISTINCT FROM` the new value, and a second `MERGE` that flags
  schools no longer in `all_school_master` with `is_deleted = TRUE` / `deleted_at`. Rows are never
  deleted; a school that returns to the master is un-flagged by the next run.

The two scripts' SELECTs must stay identical. The SELECT keeps exactly one row per
`(country, school_id_giga)` (deterministic `ROW_NUMBER`), because `all_school_master` and some
lookups contain duplicates and a `MERGE` source must be unique per key. The map columns are the
exact top 10 of the stored counts, most-frequent first. Downstream counts of schools should
filter `COALESCE(is_deleted, FALSE) = FALSE`.

### Chunked bootstrap

`create/all_ping_hourly_incremental.sql` (the one-time bootstrap) is also non-standard: unlike
every other `create/` script here, which is a single unbounded statement, it's a sequence of
one `CREATE TABLE` plus several `INSERT`s, each bounded to a disjoint, hour-aligned local-time
window — a fix for a production `EXCEEDED_LOCAL_MEMORY_LIMIT` failure when the full unbounded
history was aggregated in one `GROUP BY`. See the header comment in that file for the chunk
sizing and stop-point reasoning.

### Schema changes on incremental tables

`create/` never re-runs on an existing table (`assets.py` `_run_incremental` only
picks `create/` when `information_schema.tables` has no row). There is no schema
diffing. Editing `create/` therefore has **no effect on prod** until the table is
dropped, which we do not do (see below).

Steps 1–4 `update/` scripts are positional `INSERT INTO <table> SELECT …` with
**no column list**. New columns must be appended **last** in both `create/` and
`update/`, in the same order, and the live table must be `ALTER TABLE … ADD
COLUMN`’d **in the same paused window as the code deploy** (Phase 1 below). Either way round,
the next hourly INSERT fails on column count. A failure is harmless — the id
watermark self-heals — but avoid it.

Steps 1 and 2 (`all_gmeter_only_measurements`, `all_mlab_only_measurements`)
must stay column-aligned. Three sites do a positional `m1.* UNION ALL m2.*`:
`all_gigameter_valid_test_checker`, the `measure` CTE in
`all_gigameter_measurement_data`, and daily `all_gigameter_measurement_data_tb_physical`.
Alter both tables back-to-back. Step 2 gets typed `CAST(NULL AS …)` when the
source field is GigaMeter-only.

**Do not rebuild** an incremental table to change its schema. The incremental
runner does not delete the ADLS folder (`delete_adls_data_on_drop=False`), so a
Trino `DROP` leaves `warehouse/<table>/_delta_log` behind and the next `CREATE
TABLE … WITH (location=…)` fails or reattaches the old log. Bootstrap caps
(`LIMIT 200000` / `40000`) then take days to catch up, and Step 4’s shared
`MAX(measurement_id)` watermark across GigaMeter and MLab can skip rows for
good. `ADD COLUMN` is metadata-only.

**Runbook** (two phases: schema first, backfill later and separately)

*Phase 1 — schema and code (one short window, ~30–60 min)*

1. Trino pre-checks (column exposure, snapshot coverage, `columnMapping`). ALTERs:
   `../migrations/add_measurement_columns_wifi_latency.sql`.
2. Merge the PR and **hold the deploy**.
3. Window: after the hh:15 incremental run. Avoid 00:15–~01:30 (Spark
   `OPTIMIZE` at 00:45 is not covered by the overlap lock) and ~04:00 (daily).
   Pause `analytics_tables_incremental_schedule` (and the daily one if near
   04:00). Confirm nothing is running.
4. `ALTER TABLE … ADD COLUMN IF NOT EXISTS` on the affected tables, new
   columns last, Steps 1 and 2 together. `DESCRIBE` each table: 68, 68 and 85.
   Steps 1 and 2 end with the same 5 columns (`wifi_unavailable_reason`,
   `ssid_source`, `device_type`, `device_info`, `device_context`). Step 4 has
   those, then `loaded_latency_download_ms` and `idle_latency_download_ms`.
5. Deploy. Unpause. Materialise one incremental run by hand and check that new
   rows are populated. New rows are then filled by the pipeline from here on.
6. Next daily run (04:00) rebuilds `tb_physical` / weekly. Superset: sync
   columns on every dataset (including language copies of Insights).

*Phase 2 — backfill of existing rows (separate PR / one-off, only if needed)*

Only after Phase 1 has run cleanly for a while. Use idempotent `MERGE`s
(`IS DISTINCT FROM` guard), chunked by country / `measurement_id` range, either
with the schedule paused or between hourly runs (a MERGE racing the hourly
INSERT on the same Delta table can conflict). Widening the incremental lookback
does **not** backfill — it inserts duplicates. Fields that only exist from a
given app version onward need no backfill if Phase 1 is deployed before that
version ships.

Rollback: revert the SQL and leave the new columns NULL. `DROP COLUMN` likely
needs Delta column mapping mode `name`; these tables do not have it.

---

## Scripts

| Script | Cadence | Creates Table | Purpose | Depends On |
|---|---|---|---|---|
| `all_gmeter_only_measurements.sql` | Hourly | `default.all_gmeter_only_measurements` | GigaMeter app raw measurements — incremental Step 1. Wifi / device_type / device_info / device_context columns last (TECH-11604) | `gigameter_production_db` |
| `all_mlab_only_measurements.sql` | Hourly | `default.all_mlab_only_measurements` | MLab network test measurements — incremental Step 2. Same 5 extra columns as Step 1, as typed NULLs | `gigameter_production_db` |
| `all_gigameter_valid_test_checker.sql` | Hourly | `default.all_gigameter_valid_test_checker` | Per-measurement quality validation — incremental Step 3 | Steps 1 & 2 incremental |
| `all_gigameter_measurement_data.sql` | Hourly | `default.all_gigameter_measurement_data` | Consolidated validated measurements — incremental Step 4. Passes through the Step 1/2 columns and adds loaded / idle download latency (TECH-11791) | Steps 1, 2, & 3 incremental |
| `all_ping_hourly.sql` | Hourly | `default.all_ping_hourly` | Hourly ping/uptime aggregation per device-school (`MERGE`, not `INSERT` — see above) | `gigameter_production_db.connectivity_ping_checks` |
| `all_ping_daily.sql` | Hourly (appends complete past days only) | `default.all_ping_daily` | Daily ping aggregation | `all_ping_hourly` |
| `all_gigameter_school_daily_troubleshooting.sql` | Hourly | `default.all_gigameter_school_daily_troubleshooting` | School+day summary for Superset T0 troubleshooting (`MERGE`, delta-combine) | Step 4 |
| `all_gigameter_school_measurement_stats.sql` | Hourly | `default.all_gigameter_school_measurement_stats` | **Internal** lifetime measurement aggregates per school (`MERGE`, delta-combine) — not for dashboards | Step 4 |
| `all_gigameter_registered_schools.sql` | Hourly | `default.all_gigameter_registered_schools` | School-level registration & activity summary (`MERGE` insert/update + `is_deleted` flag) | `all_gigameter_school_measurement_stats`, `all_school_master` (daily), `dailycheckapp_school`, `lstringer.*` lookups |

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

Next up for conversion: `mng_gigameter_qos_measurements` and `bra_nicbr_daily` (daily aggregations), then `all_gigameter_registered_devices` — a full device-state snapshot, which can follow the accumulator + `MERGE` pattern used for `all_gigameter_registered_schools`.
