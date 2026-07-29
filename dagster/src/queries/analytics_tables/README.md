# Analytics Tables

SQL pipeline scripts for the **Giga school connectivity monitoring platform**. Scripts run on **Trino** against production databases and are orchestrated by **Dagster**.

Scripts are organised into three folders based on their execution model:

---

## Folders

### [`daily/`](daily/README.md)
Production scripts executed once per day via Dagster. This is the main pipeline — 24 scripts covering the full measurement, registration, aggregation, and regional reporting layers.

Some of these scripts are planned for conversion to the incremental model (hourly cadence) in a future sprint. See the daily README for details.

### [`incremental/`](incremental/README.md)
Scripts that run on an **hourly cadence** using an incremental insert pattern — new records are appended rather than the full table being recreated. Currently covers the 4 core measurement pipeline steps (Steps 1–4). Additional scripts (registration, regional) will be added in future iterations.

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

## Sync Tool

Scripts are sourced from two NocoDB tables. Use the sync tool to check for drift:

```bash
export NOCODB_TOKEN=your_token
python3 scripts/nocodb_sync.py              # Compare NocoDB vs local
python3 scripts/nocodb_sync.py --sync       # Pull latest from NocoDB
python3 scripts/nocodb_sync.py --compare-github  # Compare NocoDB vs GitHub
```
