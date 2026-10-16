# Analytics Tables

SQL pipeline scripts for the **Giga school connectivity monitoring platform**. Scripts run on **Trino** against production databases and are orchestrated by **Dagster**.

Scripts are organised into three folders based on their execution model:

---

## Folders

### [`daily/`](daily/README.md)
Production scripts executed once per day via Dagster. This is the main pipeline — 19 scripts covering the full measurement, registration, aggregation, and regional reporting layers. (Steps 1–4 of the measurement pipeline, the ping tables, `all_gigameter_school_daily_troubleshooting` and `all_gigameter_registered_schools` have already moved to the incremental model below.)

Some of the remaining scripts are planned for conversion to the incremental model (hourly cadence) in a future sprint. See the daily README for details.

### [`incremental/`](incremental/README.md)
Scripts that run on an **hourly cadence** using incremental patterns — only new records are processed rather than the full history being re-scanned (id-watermark appends, `MERGE`s, and for `all_gigameter_registered_schools` a cheap hourly rebuild on top of an incremental accumulator). Currently covers the 4 core measurement pipeline steps (Steps 1–4), the ping tables, `all_gigameter_school_daily_troubleshooting`, `all_gigameter_school_measurement_stats` and `all_gigameter_registered_schools`. Additional scripts (registration, regional) will be added in future iterations.

### [`testing/`](testing/README.md)
Non-production scripts not integrated into the Dagster pipeline. Used for one-off analysis or validating new logic before promoting to `daily/` or `incremental/`.

---

## Pipeline Architecture

```
━━━━━━━━━━━━━━━━━━━━━━━━━ RAW DATA SOURCES ━━━━━━━━━━━━━━━━━━━━━━━━━
  gigameter_production_db   │  delta_lake         │  qos / qos_raw
  (measurements, schools,   │  (school_master     │  (NIC.BR, LibreRouter,
   devices, ping checks)    │   delta tables)     │   Kenya providers)
━━━━━━━━━━━━━━━━━━━━━━━━━━━┿━━━━━━━━━━━━━━━━━━━━━┿━━━━━━━━━━━━━━━━━━━
              │
              ▼
  ┌─────────────────────────────────────────────────────┐
  │  EXECUTION ENGINE                                   │
  │  NocoDB (source) ──► Dagster (orchestrator, VM)     │
  │  daily/ → once per day    incremental/ → hourly     │
  └─────────────────────────────────────────────────────┘
              │
              ▼
  Output tables in delta_lake.default.*
  Consumed by: Superset dashboards, Giga Maps, country reports
```

## Partitioning

Partitioning is declared in the same `WITH (...)` clause as the table location, and every
partitioned table here uses the same single key:

```sql
CREATE TABLE default.<table_name>
WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/<table_name>',
    partitioned_by = ARRAY['country']
)
AS ( ... );
```

In the scripts, nothing is partitioned by date or by any other column. Current status (9 of 30 tables partitioned in code):

| Folder | Tables | Partitioned (`country`) | Not partitioned | Detail |
|---|---|---|---|---|
| `daily/` | 19 | 2 | 17 | [daily/README.md → Partitioning](daily/README.md#partitioning) |
| `incremental/` | 10 | 7 (+1 live-only, see note) | 3 | [incremental/README.md → Partitioning](incremental/README.md#partitioning) |
| `testing/` | 1 | 0 | 1 | [testing/README.md → Partitioning](testing/README.md#partitioning) |

Live check (2026-10-16): every table matches its script except `superset_activity_logs`, which is
partitioned by `log_date` in prod even though its `create/` script has no `partitioned_by`.

Changing a table's partitioning:

- **Daily:** add or remove `partitioned_by` in the script. The table is dropped and recreated
  every run, so the change takes effect on the next run.
- **Incremental:** add or remove `partitioned_by` in `create/<name>.sql`. Delta fixes partitioning
  when a table is created, and `update/` scripts that `INSERT` or `MERGE` inherit it, so an
  existing table **keeps its old layout** until it is dropped and re-bootstrapped. Plan that as a
  one-off rebuild. The exception is `all_gigameter_registered_schools`: its `update/` script runs
  `CREATE OR REPLACE TABLE` and declares `partitioned_by` itself, so keep the two scripts in sync.

## Trino Session Configuration

Some scripts require session settings to handle memory pressure or large query stages:

```sql
SET SESSION mark_distinct_strategy = 'none';    -- avoids memory spikes on DISTINCT
SET SESSION query_max_stage_count = 400;        -- needed for country_versions (200+ unions)
```

## Source of Truth

These scripts are maintained in [`unicef/giga-data-analytics`](https://github.com/unicef/giga-data-analytics/tree/main/analytics-tables) — author, review, and validate changes there first (that repo has its own NocoDB sync tooling and Trino validation workflow), then port the change here via PR once it's confirmed working. Each `.sql` file here has a matching `@asset`-decorated function in `dagster/src/assets/analytics_tables/assets.py`, named identically — renaming a script requires renaming its Python function too, and updating any other asset's `deps=[AssetKey(...)]` list that references it. Adding a new script requires adding a new `@asset` function; dropping a `.sql` file in alone does nothing.

One structural difference from `giga-data-analytics`: every `CREATE TABLE` here specifies an explicit `WITH (location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/<table_name>')` clause (templated at execution time — see `_read_statements` in `assets.py`), which `giga-data-analytics`'s copies mostly omit. Preserve this when porting a script update over.

`views/` (reusable Trino views, e.g. `school_connectivity_status_vw`) is intentionally not mirrored here — views aren't rebuilt on a schedule, so they don't fit this repo's daily/incremental asset pattern. They need to be created manually, once, directly in Trino.
