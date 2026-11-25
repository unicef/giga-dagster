# Facility schema rename and NocoDB DQ checks

Notes for the shared `facility_*` / `geo_*` upload schema cutover and the NocoDB DQ configuration CSVs produced for school and health facility geolocation.

Related:

- [facility-geolocation-contracts.md](./facility-geolocation-contracts.md) — live contracts after the rename
- [facility-cutover-school-master-and-connectivity.md](./facility-cutover-school-master-and-connectivity.md) — what still writes `school_master` / connectivity RT

Source schema CSVs and generated NocoDB imports (local working copies):

`/Users/brianmusisi/Desktop/schemas and noco/`

| File | Role |
| --- | --- |
| `facilties_core schema.csv` | Shared core upload columns |
| `schools schema.csv` | School extension only |
| `health schema.csv` | Health extension only |
| `SchoolGeolocationMasterDQChecks.csv` | Updated school DQ table for NocoDB import |
| `HealthFacilityGeolocationDQChecks.csv` | New health DQ table for NocoDB import |

## Decisions

1. **School uploads switch to facility names end-to-end.** Live contract uses `facility_id_govt`, `facility_name`, `geo_latitude`, `geo_longitude`, etc. Old names (`school_id_govt`, `school_name`, `latitude`, `longitude`, …) are dropped from the upload/`SCHOOL_SPEC` public contract.
2. **DQ rows for columns not in the new schemas are dropped** (e.g. teachers, classrooms, latrines, collection modality, `download_speed_govt`).
3. **`facility_level` is kept** as the Giga-normalized form of `facility_level_govt` (was `education_level` ← `education_level_govt`). Both school and health NocoDB tables include a `facility_level` domain check.
4. **Check engines use `facility_*` / `geo_*` names directly.** The shared engines in `dagster/src/data_quality_checks/` read and emit facility/geo columns and `dq_*` flags, so `facility.py` no longer aliases school/`latitude` names. Coverage, adhoc master/QoS, and related config lists use `facility_id_*` / `geo_*` as well. Live Delta/registry column migrations may still be required before those jobs run against existing data.

## Column contract

### Shared core (`facility_core` + upload profile)

**Pipeline-owned**

- `facility_id_giga`
- `facility_level` (from `facility_level_govt` via Noco name-mappings)
- `country_code`
- Derived admin / disputed columns when enrichment runs: `admin1`…`admin4`, `admin*_id_giga`, `disputed_region`

**Upload / shared profile** (from facilities core schema)

- Identity: `facility_id_govt`, `facility_id_type_govt`, `facility_id_building_govt`, `facility_name`, `facility_level_govt`, `facility_ownership_type`, `facility_area_type_govt`, `facility_is_open`, `facility_establishment_year`, `facility_address`, `facility_data_source`
- Geo: `geo_latitude`, `geo_longitude`, `geo_admin1_govt`, `geo_admin2_govt`
- Connectivity / electricity / buildings / devices / staff / refugee fields as listed in the core schema CSV

### School extension (`school_facility_details`)

Only:

- `computer_lab_availability`
- `students_num`
- `students_female_num`
- `students_male_num`
- `students_other_num`

### Health extension (`health_facility_details`)

Only fields from the health schema CSV:

- `facility_id_2_govt`, `facility_id_2_type_govt`
- `facility_health_type_govt`, `facility_health_care_type`, `facility_health_service_type`
- `system_hmis`, `system_hmis_used`, `system_emr`, `system_emr_used`
- `outpatients_avg`, `outpatients_ref_period`, `inpatients_avg`, `inpatients_ref_period`
- `pop_catchment_govt`

## Key renames

| Old | New |
| --- | --- |
| `school_id_govt` / `school_name` | `facility_id_govt` / `facility_name` |
| `education_level_govt` / `education_level` | `facility_level_govt` / `facility_level` |
| `latitude` / `longitude` | `geo_latitude` / `geo_longitude` |
| `school_id_govt_type` | `facility_id_type_govt` |
| `building_id_govt` | `facility_id_building_govt` |
| `school_area_type` / `school_funding_type` | `facility_area_type_govt` / `facility_ownership_type` |
| `water_availability` / `electricity_type` | `facility_water_availability` / `electricity_type_govt` |
| `download_speed_contracted` | `connectivity_down_speed_contracted` |
| `computer_lab` / `num_students*` / `num_computers` | `computer_lab_availability` / `students_*` / `device_computers_num` |
| `dq_is_school_density_*` / `dq_duplicate_all_except_school_code` | `dq_is_facility_density_*` / `dq_duplicate_all_except_facility_code` |

## NocoDB DQ CSVs

Both CSVs use the same header as the previous school export:

`Related Check ID`, `DQ Table Column Name`, `Critical For`, `DQ Check Category`, `Column Checked`, `Create DQ`, `Update DQ`, `Human Readable Name`, `Value in DQ report if check is passed`, `Inverse name`, `Complete explanation for the dictionary`, `Assertion`, `Column Key`, `UI Error Description`, `DQR Warnings`

### `SchoolGeolocationMasterDQChecks.csv`

- Remapped `Column Checked`, `DQ Table Column Name`, human-readable text, and duplicate-set keys to the new names.
- Coordinates use `geo_latitude` / `geo_longitude`.
- Keeps critical / location / precision / domain / range / consistency checks that still have schema homes.
- Keeps `facility_level` domain check (Giga-normalized).
- Student / device relation checks remapped; teacher / classroom / latrine / modality / `download_speed_govt` / etc. removed.
- Density and full-row duplicate checks use `facility` wording.

### `HealthFacilityGeolocationDQChecks.csv` (new)

- Same header shape as school.
- Shared core critical / geo / location / precision / connectivity-electricity domain checks.
- Includes `facility_level` domain check.
- Health-extension null / domain / range checks for health-only columns.
- No school-only student / `computer_lab_availability` checks.

### Ops follow-up in NocoDB

1. Import / replace `SchoolGeolocationMasterDQChecks` from the updated CSV.
2. Create `HealthFacilityGeolocationDQChecks` and register its name→ID in the NocoDB name-mappings table (`NOCODB_NAME_MAPPINGS_TABLE_ID`).
3. Update govt→giga name-mapping rows so `facility_level_govt` drives `facility_level` (replacing `education_level_govt` → `education_level`).
4. Validate that every `DQ Table Column Name` referenced in Noco exists in pipeline DQ output before enabling sensors.

## Code changes (facility pipeline)

| Area | What changed |
| --- | --- |
| `dagster/src/facilities/specs.py` | Shared core + school/health extensions; both specs use `facility_id_*` / `facility_name` / `facility_level` / `geo_*`; required core coords are `geo_latitude`/`geo_longitude`; refreshed `dq_domains` / `dq_ranges` |
| `dagster/src/data_quality_checks/*` | Check engines renamed to `facility_*` / `geo_*` columns and flags (`facility_density_check`, `extract_facility_id_govt_duplicates`); `facility.py` alias/rename layer removed |
| `dagster/src/assets/facility_geolocation/factory.py` | Map generation aliases `geo_*` → lat/lon (map generator still expects `latitude`/`longitude`); duplicates report already uses facility names |
| Facility tests | Specs / bronze / publish fixtures use new column names |
| `docs/facility-geolocation-contracts.md` | Contracts updated for `geo_*` and facility identity |

Out of scope unless blocking facility load: QoS, connectivity RT, coverage, and other paths still hard-wired to `school_*` / `school_master`.

## Upload / metadata paths (unchanged by this rename)

Sensor and ingestion still agree on:

- File: `raw/uploads/facility-geolocation/<ISO3>/<schema_key>/<filename>`
- Metadata: `raw/upload_metadata/facility-geolocation/<ISO3>/<schema_key>/<filename>.metadata.json`

School `schema_key`: `schools`. Health `schema_key`: `health_facilities`.

## Verification done

- Docker pytest on facility specs / bronze / publish (+ school geolocation definition/upload tests) passed after the rename.
- Pre-commit (ruff / format / bandit / secrets) passed on the touched files after complexity refactors in `facility.py`.
