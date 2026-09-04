# Analytics Tables

SQL pipeline scripts for the **Giga school connectivity monitoring platform**. Scripts run on **Trino** against production databases and are orchestrated by **Dagster**.

Scripts are organised into three folders based on their execution model:

---

## Folders

### [`daily/`](daily/README.md)
Production scripts executed once per day via Dagster. This is the main pipeline — 20 scripts covering the full measurement, registration, aggregation, and regional reporting layers. (Steps 1–4 of the measurement pipeline and the ping tables have already moved to the incremental model below.)

Some of the remaining scripts are planned for conversion to the incremental model (hourly cadence) in a future sprint. See the daily README for details.

### [`incremental/`](incremental/README.md)
Scripts that run on an **hourly cadence** using an incremental insert pattern — new records are appended rather than the full table being recreated. Currently covers the 4 core measurement pipeline steps (Steps 1–4) plus the ping tables. Additional scripts (registration, regional) will be added in future iterations.

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
