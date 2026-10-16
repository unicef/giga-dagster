# Testing Scripts

Non-production scripts not integrated into the Dagster pipeline. Used for one-off analysis or validating new logic before promoting to [`daily/`](../daily/README.md) or [`incremental/`](../incremental/README.md).

Scripts are sourced from the **Physical Tables** NocoDB table (`mzq6hlnsicq1r0v`, `Integrated in pipeline = False`).

---

## Scripts

| Script | Output Table | Purpose |
|---|---|---|
| `superset_session_lengths.sql` | `public.superset_session_lengths` | Calculates Superset user session lengths by organisation, used to analyse dashboard engagement |

---

## Partitioning

No testing tables are partitioned.

| Script | Output Table | Partitioned | Partitioned by |
|---|---|---|---|
| `superset_session_lengths.sql` | `public.superset_session_lengths` | No | — |

The script has no `WITH (...)` clause, so it sets neither `partitioned_by` nor `location`. When
promoting a testing script to `daily/` or `incremental/`, add the `WITH (location = …)` clause
(and `partitioned_by = ARRAY['country']` if the table has a `country` column and is large). See
[the root README](../README.md#partitioning).

---

## How to Run

Testing scripts run in the same Trino environment as production but write to separate tables. They are not executed by Dagster — run manually via the Trino CLI or a notebook.

```sql
SET SESSION mark_distinct_strategy = 'none';
SET SESSION query_max_stage_count = 400;
```
