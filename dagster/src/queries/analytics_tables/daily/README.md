# Daily Scripts

Production scripts executed **once per day** via Dagster (5:15 AM). All scripts use a drop-and-recreate pattern — the full table is rebuilt on each run.

Scripts are maintained in [`unicef/giga-data-analytics`](https://github.com/unicef/giga-data-analytics/tree/main/analytics-tables/daily) (itself sourced from the **Physical Tables** NocoDB table, `mzq6hlnsicq1r0v`, `Integrated in pipeline = True`) and ported here via PR once validated there.

---

## Script Reference

| Script | Creates Table | Purpose | Key Dependencies | Incremental planned |
|---|---|---|---|---|
| `country_versions.sql` | `default.country_versions` | Latest delta version per country | `delta_lake.school_master.*` | |
| `all_school_master.sql` | `default.all_school_master` | Master school reference (~2.1M schools, 69 columns) | `country_versions` | |
| `all_gmeter_only_measurements.sql` | `default.all_gmeter_only_measurements` | GigaMeter app raw measurements (Step 1) | `gigameter_production_db` | Yes — Step 1 incremental in progress |
| `all_mlab_only_measurements.sql` | `default.all_mlab_only_measurements` | MLab network test measurements (Step 2) | `gigameter_production_db` | Yes — Step 2 incremental in progress |
| `all_gigameter_valid_test_checker.sql` | `default.all_gigameter_valid_test_checker` | Per-measurement quality validation (Step 3) | Steps 1 & 2 | Yes — Step 3 incremental in progress |
| `isp_asn_country_mapping.sql` | `default.isp_asn_country_mapping` | Canonical ISP name/ASN lookup, resolves ASN-truncation variants per country | Steps 1 & 2 | |
| `all_gigameter_measurement_data.sql` | `default.all_gigameter_measurement_data` | Consolidated validated measurements (Step 4) | Steps 1, 2, & 3, ISP/ASN mapping | Yes — Step 4 incremental in progress |
| `all_gigameter_measurement_data_daily.sql` | `default.all_gigameter_measurement_data_daily` | Daily aggregation per school+device (weekdays only) — p5/p50/p95 speed metrics, summed data volume, JSON categorical breakdowns, school attributes | `all_gigameter_measurement_data` | |
| `all_gigameter_measurement_data_weekly.sql` | `default.all_gigameter_measurement_data_weekly` | Weekly aggregation per school+device (weekdays only) — identical column structure to daily table | `all_gigameter_measurement_data` | |
| `all_gigameter_measurement_data_tb_physical.sql` | `default.all_gigameter_measurement_data_tb_physical` | Backwards-compatible measurement data variant | Steps 1 & 2 | |
| `all_ping_hourly.sql` | `default.all_ping_hourly` | Hourly ping/uptime aggregation per device-school | `gigameter_production_db.connectivity_ping_checks` | |
| `all_ping_daily.sql` | `default.all_ping_daily` | Daily ping aggregation | `all_ping_hourly` | |
| `all_gigameter_inc_ping_daily.sql` | `default.all_gigameter_inc_ping_daily` | Daily roll-up combining ping + speed test data | `all_ping_hourly`, measurement data | |
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
| `all_gigameter_school_daily_troubleshooting.sql` | `default.all_gigameter_school_daily_troubleshooting` | Pre-aggregated school+day summary for Superset T0 troubleshooting | `all_gigameter_measurement_data_incremental` (separate hourly pipeline) | |

---

## Execution Order

Scripts are executed in dependency order via the `deps=[AssetKey(...)]` declarations in `dagster/src/assets/analytics_tables/assets.py`. The list below mirrors that order for reference — if it drifts from `assets.py`, `assets.py` is authoritative.

```
 1. country_versions
 2. all_school_master                          ← #1
 3. all_gmeter_only_measurements
 4. all_mlab_only_measurements
 5. all_gigameter_valid_test_checker           ← #3, #4
 6. isp_asn_country_mapping                    ← #3, #4
 7. all_gigameter_measurement_data             ← #2, #3, #4, #5, #6
 8. all_gigameter_measurement_data_tb_physical ← #2, #3, #4
 9. all_gigameter_measurement_data_daily       ← #7
10. all_gigameter_measurement_data_weekly      ← #7
11. all_gigameter_appversion_funnel            ← #2, #7
12. all_gigameter_registered_tb_physical       ← #2, #8, #11
13. all_gigameter_registered_schools           ← #2, #7
14. bra_nicbr_daily
15. bra_nicbr_registered_schools
16. all_gigameter_funnelsummary                ← #2
17. bra_benchmarkstatus_wow                    ← #14
18. all_gigamaps_realtimeconnectivity
19. mng_gigameter_qos_measurements             ← #7
20. mng_gigameter_qos_registered               ← #7, #11, #13
21. all_gigameter_registered_devices           ← #2, #7, #11
22. all_ping_hourly
23. all_ping_daily                             ← #22
24. all_gigameter_inc_ping_daily               ← #7, #11, #23
25. all_gigameter_school_consistency_history   ← #3, #7
26. all_gigameter_school_daily_troubleshooting ← incremental pipeline (all_gigameter_measurement_data_incremental), not part of this daily chain
```

---

## Aggregated Tables and Dashboard Usage

`all_gigameter_measurement_data_daily` and `all_gigameter_measurement_data_weekly` are pre-aggregated rollups of the raw measurement table, designed to reduce dashboard query times. Approximately half of dashboard charts can run from these instead of row-level data.

**Migration status:**
- New country dashboard: already using aggregated tables
- Old v3 Superset dashboards: still pointing to `all_gigameter_measurement_data` — need updating

---

## Incremental Conversion Roadmap

The four core measurement steps (Steps 1–4) have incremental variants under [`../incremental/`](../incremental/README.md) currently being validated. Once confirmed, these daily scripts will be retired in favour of the hourly incremental versions.

A second iteration will convert the registration and regional scripts (`all_gigameter_registered_schools`, `all_gigameter_registered_devices`, `bra_nicbr_*`, `mng_gigameter_*`) to the incremental model.
