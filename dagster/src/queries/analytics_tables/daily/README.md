# Daily Scripts

Production scripts executed **once per day** via Dagster (5:15 AM). All scripts use a drop-and-recreate pattern — the full table is rebuilt on each run.

Scripts are sourced from the **Physical Tables** NocoDB table (`mzq6hlnsicq1r0v`, `Integrated in pipeline = True`).

---

## Script Reference

| Script | Creates Table | Purpose | Key Dependencies | Incremental planned |
|---|---|---|---|---|
| `country_versions.sql` | `default.country_versions` | Latest delta version per country | `delta_lake.school_master.*` | |
| `all_school_master.sql` | `default.all_school_master` | Master school reference (~2.1M schools, 69 columns) | `country_versions` | |
| `all_gmeter_only_measurements.sql` | `default.all_gmeter_only_measurements` | GigaMeter app raw measurements (Step 1) | `gigameter_production_db` | Yes — Step 1 incremental in progress |
| `all_mlab_only_measurements.sql` | `default.all_mlab_only_measurements` | MLab network test measurements (Step 2) | `gigameter_production_db` | Yes — Step 2 incremental in progress |
| `all_gigameter_valid_test_checker.sql` | `default.all_gigameter_valid_test_checker` | Per-measurement quality validation (Step 3) | Steps 1 & 2 | Yes — Step 3 incremental in progress |
| `all_gigameter_measurement_data.sql` | `default.all_gigameter_measurement_data` | Consolidated validated measurements (Step 4) | Steps 1, 2, & 3 | Yes — Step 4 incremental in progress |
| `all_gigameter_measurement_data_daily.sql` | `default.all_gigameter_measurement_data_daily` | Daily aggregation per school (weekdays only) | `all_gigameter_measurement_data` | |
| `all_gigameter_measurement_data_weekly.sql` | `default.all_gigameter_measurement_data_weekly` | Weekly aggregation per school with percentile speeds | `all_gigameter_measurement_data` | |
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
| `all_gigameter_funnelsummary_tb_physical.sql` | `default.all_gigameter_funnelsummary_tb_physical` | Country-level adoption funnel & QoS source summary | measurement data, `all_school_master`, QoS tables | |
| `bra_nicbr_registered_tb.sql` | `default.bra_nicbr_registered_tb` | Brazil school NIC.BR registration summary | `school_master.bra`, `qos.bra`, FUST | Planned (2nd iteration) |
| `bra_nicbr_daily_tb.sql` | `default.bra_nicbr_daily_tb` | Daily Brazil NIC.BR speed data with benchmark flags | `qos.bra`, `school_master.bra`, FUST | Planned (2nd iteration) |
| `bra_benchmarkstatus_wow.sql` | `default.bra_benchmarkstatus_wow` | Brazil weekly benchmark status with WoW changes | `bra_nicbr_daily_tb` | |
| `mng_gigameter_qos_registered.sql` | `default.mng_gigameter_qos_registered` | Mongolia school registration (GigaMeter + LibreRouter) | `qos_raw.mng`, `school_master.mng` | Planned (2nd iteration) |
| `mng_gigameter_qos_measurements.sql` | `default.mng_gigameter_qos_measurements` | Mongolia daily measurements (GigaMeter + LibreRouter) | `qos.mng`, `school_master.mng` | Planned (2nd iteration) |

---

## Execution Order

Scripts are executed sequentially in the order they appear in the NocoDB physical tables view. Dependencies are noted where relevant — these must be respected if the order is ever changed.

```
 1. country_versions
 2. all_school_master                          ← #1
 3. all_gmeter_only_measurements
 4. all_mlab_only_measurements
 5. all_gigameter_valid_test_checker           ← #3, #4
 6. all_gigameter_measurement_data             ← #2, #3, #4, #5
 7. all_gigameter_measurement_data_tb_physical ← #2, #3, #4
 8. all_gigameter_measurement_data_daily       ← #6
 9. all_gigameter_measurement_data_weekly      ← #6
10. all_gigameter_appversion_funnel            ← #2, #6
11. all_gigameter_registered_tb_physical       ← #2, #7, #10
12. all_gigameter_registered_schools           ← #2, #6
13. bra_nicbr_daily_tb
14. bra_nicbr_registered_tb
15. all_gigameter_funnelsummary_tb_physical    ← #2
16. bra_benchmarkstatus_wow                    ← #13
17. all_gigamaps_realtimeconnectivity
18. mng_gigameter_qos_measurements             ← #7
19. mng_gigameter_qos_registered               ← #11
20. all_gigameter_registered_devices           ← #2, #6, #10
21. all_ping_hourly
22. all_ping_daily                             ← #21
23. all_gigameter_inc_ping_daily               ← #6, #10, #22
24. all_gigameter_school_consistency_history   ← #3, #6
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
