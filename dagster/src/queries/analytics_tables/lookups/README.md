# Lookups

Small reference tables that other analytics tables join to. They are **not Dagster assets**: nothing here runs on a schedule. Each script rebuilds its table from explicit `VALUES`, so the repo holds the reviewed contents of the table.

| Script | Table | Rows | Read by |
|---|---|---|---|
| `country_geography.sql` | `custom_dataset.country_geography` | 249 | `daily/all_school_master.sql`, `daily/all_gigameter_funnelsummary.sql` |
| `country_timezones.sql` | `default.country_timezones` | 249 | local-time fields (`timestamp_local`) |

One row per Giga Maps country (`gigamaps_production_db.public.locations_country`), keyed on ISO3.

## Changing a lookup
1. Edit the `VALUES` in the script and open a PR (one row per country; keep ISO3 unique).
2. Keep `country_geography` names **ASCII only**: `all_school_master` is partitioned by `country`, and an accented name fails its rebuild with `HIVE_INVALID_PARTITION_VALUE`.
3. After merge, run it manually on prod (steps in the script header), then rebuild `all_school_master` and `all_gigameter_funnelsummary`.

Don't edit these tables through GigaSync uploads or ad-hoc `INSERT`s: the repo copy then drifts from prod.
