# Daily Scripts

Production scripts executed **once per day** via Dagster (5:15 AM). All scripts use a drop-and-recreate pattern — the full table is rebuilt on each run.

Scripts are maintained in [`unicef/giga-data-analytics`](https://github.com/unicef/giga-data-analytics/tree/main/analytics-tables/daily) (itself sourced from the **Physical Tables** NocoDB table, `mzq6hlnsicq1r0v`, `Integrated in pipeline = True`) and ported here via PR once validated there.

**Steps 1–4 of the measurement pipeline and the ping tables are no longer produced here.**
`all_gmeter_only_measurements`, `all_mlab_only_measurements`, `all_gigameter_valid_test_checker`,
`all_gigameter_measurement_data`, `all_ping_hourly`, and `all_ping_daily` — and their `@asset`
Python functions — are now produced by the hourly/daily incremental pipeline in
[`../incremental/`](../incremental/README.md); the daily scripts and assets that used to create
them have been retired. Every other script below that reads one of these tables
(`isp_asn_country_mapping`, `all_gigameter_measurement_data_tb_physical`,
`all_gigameter_measurement_data_daily`/`_weekly`, `all_gigameter_appversion_funnel`,
`all_gigameter_registered_schools`/`_devices`, `all_gigameter_school_consistency_history`,
`all_gigameter_funnelsummary`, `mng_gigameter_qos_*`, `all_gigameter_inc_ping_daily`) is unchanged
and keeps reading the same table names via `deps=[AssetKey(["incremental", ...])]` — only who
populates them changed. Mirrors `unicef/giga-data-analytics#25`.

---

## Script Reference

| Script | Creates Table | Purpose | Key Dependencies | Incremental planned |
|---|---|---|---|---|
| `country_versions.sql` | `default.country_versions` | Latest delta version per country | `delta_lake.school_master.*` | |
| `all_school_master.sql` | `default.all_school_master` | Master school reference (~2.1M schools, 69 columns) | `country_versions` | |
| `isp_asn_country_mapping.sql` | `default.isp_asn_country_mapping` | Canonical ISP name/ASN lookup, resolves ASN-truncation variants per country | `all_gmeter_only_measurements`, `all_mlab_only_measurements` (incremental pipeline) | |
| `all_gigameter_measurement_data_daily.sql` | `default.all_gigameter_measurement_data_daily` | Daily aggregation per school+device — p5/p50/p95 speed metrics, summed data volume, JSON categorical breakdowns, school attributes | `all_gigameter_measurement_data` (incremental pipeline) | |
| `all_gigameter_measurement_data_weekly.sql` | `default.all_gigameter_measurement_data_weekly` | Weekly aggregation per school+device (weekdays only) — identical column structure to daily table | `all_gigameter_measurement_data` (incremental pipeline) | |
| `all_gigameter_measurement_data_tb_physical.sql` | `default.all_gigameter_measurement_data_tb_physical` | Backwards-compatible measurement data variant | `all_gmeter_only_measurements`, `all_mlab_only_measurements` (incremental pipeline) | |
| `all_gigameter_inc_ping_daily.sql` | `default.all_gigameter_inc_ping_daily` | Daily roll-up combining ping + speed test data | `all_ping_daily` (incremental pipeline), measurement data | |
| `all_gigamaps_realtimeconnectivity.sql` | `default.all_gigamaps_realtimeconnectivity` | Daily school connectivity status by country | `gigamaps_production_db` | |
| `all_gigameter_appversion_funnel.sql` | `default.all_gigameter_appversion_funnel` | Device registration → data sent → version upgrade funnel | `gigameter_production_db`, `all_school_master` | |
| `all_gigameter_registered_tb_physical.sql` | `default.all_gigameter_registered_tb_physical` | Registered schools/devices combined physical table | `gigameter_production_db`, `all_school_master` | |
| `all_gigameter_registered_schools.sql` | `default.all_gigameter_registered_schools` | School-level registration & activity summary | measurement data, `all_school_master` | Planned (2nd iteration) |
| `all_gigameter_registered_devices.sql` | `default.all_gigameter_registered_devices` | Device-level registration & activity summary | measurement data, appversion funnel | Planned (2nd iteration) |
| `all_gigameter_school_consistency_history.sql` | `default.all_gigameter_school_consistency_history` | Weekly 30-day rolling consistency score per school | measurement data | |
| `all_gigameter_funnelsummary.sql` | `default.all_gigameter_funnelsummary` | Country-level adoption funnel & QoS source summary | measurement data, `all_school_master`, QoS tables | |
| `bra_nicbr_registered_schools.sql` | `default.bra_nicbr_registered_schools` | Brazil school NIC.BR registration summary | `school_master.bra`, `qos.bra`, FUST | Planned (2nd iteration) |
| `bra_nicbr_daily.sql` | `default.bra_nicbr_daily` | Daily Brazil NIC.BR speed data with benchmark flags | `qos.bra`, `school_master.bra`, FUST | Planned (2nd iteration) |
| `bra_benchmarkstatus_wow.sql` | `default.bra_benchmarkstatus_wow` | Brazil weekly benchmark status with WoW changes | `bra_nicbr_daily` | |
| `mng_gigameter_qos_registered.sql` | `default.mng_gigameter_qos_registered` | Mongolia school registration (GigaMeter + LibreRouter) | `qos_raw.mng`, `school_master.mng` | Planned (2nd iteration) |
| `mng_gigameter_qos_measurements.sql` | `default.mng_gigameter_qos_measurements` | Mongolia daily measurements (GigaMeter + LibreRouter) | `qos.mng`, `school_master.mng` | Planned (2nd iteration) |
| `all_gigameter_school_daily_troubleshooting.sql` | `default.all_gigameter_school_daily_troubleshooting` | Pre-aggregated school+day summary for Superset T0 troubleshooting | `all_gigameter_measurement_data` (incremental pipeline) | |
| `all_gigameter_school_percentile_distributions.sql` | `default.all_gigameter_school_percentile_distributions` | Per-school percentile distributions (p25–p95) of speed test metrics, in-school-hours only | `all_gigameter_measurement_data` (incremental pipeline) | |
| `superset_activity_logs.sql` | `default.superset_activity_logs` | Per-event Superset activity log enriched with user role/org and dashboard/chart context | `superset` Postgres connector | |
| `superset_session_lengths.sql` | `default.superset_session_lengths` | Superset user sessions derived from the activity log via 30-minute gap-splitting | `superset` Postgres connector | |

---

## Execution Order

Scripts are executed in dependency order via the `deps=[AssetKey(...)]` declarations in `dagster/src/assets/analytics_tables/assets.py`. The list below mirrors that order for reference — if it drifts from `assets.py`, `assets.py` is authoritative.

```
 1. country_versions
 2. all_school_master                          ← #1
 3. isp_asn_country_mapping                    ← incremental pipeline (all_gmeter_only_measurements, all_mlab_only_measurements)
 4. all_gigameter_measurement_data_tb_physical ← #2, incremental pipeline (all_gmeter_only_measurements, all_mlab_only_measurements)
 5. all_gigameter_measurement_data_daily       ← incremental pipeline (all_gigameter_measurement_data)
 6. all_gigameter_measurement_data_weekly      ← incremental pipeline (all_gigameter_measurement_data)
 7. all_gigameter_appversion_funnel            ← #2, incremental pipeline (all_gigameter_measurement_data)
 8. all_gigameter_registered_tb_physical       ← #2, #4, #7
 9. all_gigameter_registered_schools           ← #2, incremental pipeline (all_gigameter_measurement_data)
10. bra_nicbr_daily
11. bra_nicbr_registered_schools
12. all_gigameter_funnelsummary                ← #2, incremental pipeline (all_gmeter_only_measurements, all_mlab_only_measurements)
13. bra_benchmarkstatus_wow                    ← #10
14. all_gigamaps_realtimeconnectivity
15. mng_gigameter_qos_measurements             ← incremental pipeline (all_gigameter_measurement_data)
16. mng_gigameter_qos_registered               ← incremental pipeline (all_gigameter_measurement_data), #7, #9
17. all_gigameter_registered_devices           ← #2, incremental pipeline (all_gigameter_measurement_data), #7
18. all_gigameter_inc_ping_daily               ← incremental pipeline (all_gigameter_measurement_data, all_ping_daily), #7
19. all_gigameter_school_consistency_history   ← incremental pipeline (all_gmeter_only_measurements, all_gigameter_measurement_data)
20. all_gigameter_school_daily_troubleshooting ← incremental pipeline (all_gigameter_measurement_data), not part of this daily chain
21. all_gigameter_school_percentile_distributions ← incremental pipeline (all_gigameter_measurement_data), not part of this daily chain
22. superset_activity_logs                       ← superset Postgres connector, no giga-dagster deps
23. superset_session_lengths                     ← superset Postgres connector, no giga-dagster deps
```

Steps 3–4, 7, 9, 12, 15–19 depend on tables produced by the incremental pipeline
(`../incremental/`) rather than by another step in this chain — those dependencies are satisfied
by the incremental Dagster assets, not by execution order here.

---

## Aggregated Tables and Dashboard Usage

`all_gigameter_measurement_data_daily` and `all_gigameter_measurement_data_weekly` are pre-aggregated rollups of the raw measurement table, designed to reduce dashboard query times. Approximately half of dashboard charts can run from these instead of row-level data.

**Migration status:**
- New country dashboard: already using aggregated tables
- Old v3 Superset dashboards: still pointing to `all_gigameter_measurement_data` — need updating

---

## Incremental Conversion Roadmap

The four core measurement steps (Steps 1–4) and the two ping tables have been cut over to the
hourly/daily incremental pipeline under [`../incremental/`](../incremental/README.md) — the daily
scripts and `@asset` functions that used to produce `all_gmeter_only_measurements`,
`all_mlab_only_measurements`, `all_gigameter_valid_test_checker`, `all_gigameter_measurement_data`,
`all_ping_hourly`, and `all_ping_daily` have been retired from this directory.

A second iteration will convert the registration and regional scripts (`all_gigameter_registered_schools`, `all_gigameter_registered_devices`, `bra_nicbr_*`, `mng_gigameter_*`) to the incremental model.
