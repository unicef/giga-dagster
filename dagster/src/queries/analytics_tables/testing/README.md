# Testing Scripts

Non-production scripts not integrated into the Dagster pipeline. Used for one-off analysis or validating new logic before promoting to [`daily/`](../daily/README.md) or [`incremental/`](../incremental/README.md).

Scripts are sourced from the **Physical Tables** NocoDB table (`mzq6hlnsicq1r0v`, `Integrated in pipeline = False`).

---

## Scripts

| Script | Output Table | Purpose |
|---|---|---|
| `superset_session_lengths.sql` | `public.superset_session_lengths` | Calculates Superset user session lengths by organisation, used to analyse dashboard engagement |

---

## How to Run

Testing scripts run in the same Trino environment as production but write to separate tables. They are not executed by Dagster — run manually via the Trino CLI or a notebook.

```sql
SET SESSION mark_distinct_strategy = 'none';
SET SESSION query_max_stage_count = 400;
```
