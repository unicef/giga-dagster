# Facility cutover: `school_master`, connectivity RT, and bronze enrichment

Notes for the school/health facility geolocation cutover on `feat/multi-facility-dq-pipeline`. After school geolocation moved onto the facility factory, uploads no longer publish to `school_master` / `school_reference`. Gold is `facility_core` plus a facility-specific extension table.

Related: [facility-geolocation-contracts.md](./facility-geolocation-contracts.md).

## What still writes `school_master`

Geolocation (school and health) does **not** write `school_master` anymore. These paths still do:

| Path | Asset / entrypoint | What it writes |
| --- | --- | --- |
| Coverage post-approval | `common.master` | Merges coverage columns into `school_master.<CC>` |
| Connectivity RT apply | `school_connectivity_realtime_master` | Coalesces `connectivity` / `connectivity_RT*` onto `school_master.<CC>` |
| Adhoc | CSV→gold, connectivity_type remap, mock CDF, etc. | Read/write `school_master` |

Also updated by the RT path (not gold master, but school-specific): `school_geolocation_silver.<cc>`.

### Readers (not writers)

These still **read** or join `school_master` (or analytics views built from it):

- QoS / VCT jobs
- Analytics (`all_school_master`, country versions, funnels)
- Geolocation APIs / deletion helpers
- Master release notes and DQ-kit master export (when those assets run)

Coverage and connectivity remain the live owners of `school_master` until a separate cutover moves them onto `facility_core`.

## Connectivity RT is hard-wired to school tables

The live RT updater is not parameterized by facility type. Destinations and join keys are fixed.

### Sensor destinations

In `dagster/src/sensors/school_connectivity.py`:

- Silver target: `warehouse/school_geolocation_silver.db/<cc>` (`metastore_schema=school_geolocation`)
- Master target: `warehouse/school_master.db/<cc>` (`metastore_schema=school_master`)
- Domain/dataset constants: `DOMAIN = "school"`, `DATASET_TYPE = "geolocation"`

### Apply join

In `dagster/src/assets/school_connectivity/assets.py`, RT CSV rows join current silver/master on:

```text
current.school_id_giga == updated.school_id_giga_updated
```

Columns coalesced onto the target:

- `connectivity` (set to `"Yes"` for matched RT schools)
- `connectivity_RT`
- `connectivity_RT_datasource`
- `connectivity_RT_ingestion_timestamp`

### Live RT flow (school-only today)

```text
daily schedule
  → build RT universe (Gigameter / MLAB / QoS) keyed by school_id_govt + country
  → pipeline_tables.school_connectivity_realtime_schools
  → CSV under raw/connectivity_updates/
  → update school_geolocation_silver.<cc>  (join school_id_giga)
  → update school_master.<cc>               (same join)
```

There is no parallel path yet that writes `facility_core` / `health_facility_details` using `facility_id_giga`.

## Spec flags: `enable_connectivity` / `enable_registration_updates`

Defined on `FacilityGeolocationSpec` in `dagster/src/facilities/specs.py`:

| Spec | `enable_connectivity` | `enable_registration_updates` |
| --- | --- | --- |
| `SCHOOL_SPEC` | `True` | `True` |
| `HEALTH_SPEC` | `False` (default) | `False` (default) |

### Intended meaning

These were added with the facility factory as knobs mirroring old school bronze behavior:

- **`enable_connectivity`** — on bronze/upload, merge known RT connectivity into the upload (what `merge_connectivity_to_master` does).
- **`enable_registration_updates`** — for Gigameter-sourced uploads, run `set_school_registration_verification_status` (**schools only** for now).

### Reality today

`prepare_facility_bronze` now honors these flags:

- `enable_connectivity` → upload-time RT merge via `merge_connectivity_to_master` (skipped in `LOCAL`)
- `enable_registration_updates` → `set_school_registration_verification_status` when `facility_type == "school"` and `source == "gigameter"`

## What is `STABLE_FACILITY_ID_CONTRACT`?

Defined in `dagster/src/facilities/identity.py` as the singleton `StableFacilityIdContract`.

It is the **versioned, deterministic ID generator** for all facility types. It replaces the old school-only `create_school_id_giga`.

| Property | Value |
| --- | --- |
| Algorithm | UUIDv5 over a canonical key |
| Namespace UUID | fixed (`FACILITY_ID_NAMESPACE`) |
| Version label | `facility-id-v1` |
| Canonical key parts | `facility_type` + `country_code` + `facility_id_govt` (NFKC-normalized, casefolded, whitespace-collapsed) |

Behavior:

- Same type + country + govt ID → same `facility_id_giga` every time
- Different `facility_type` (e.g. `school` vs `health`) → different IDs even with the same govt ID
- Missing/blank parts → no ID (`None`)
- `add_to_dataframe(...)` writes the ID column and can preserve an existing non-null ID

Facility bronze uses this instead of `create_school_id_giga`. That replacement stays; school ID generation is not brought back as a separate path.

## Bronze enrichment: current vs decisions

### Facility bronze (`prepare_facility_bronze`) — wired

Order of enrichment:

1. Normalize missing values + ensure country column
2. `map_govt_to_giga_columns(df, uploaded_columns)` — all facilities
3. `add_connectivity_type_columns(df, uploaded_columns)` — same parameters
4. `is_new_facility` via `add_is_new_entity`
5. Stable ID via `STABLE_FACILITY_ID_CONTRACT` (replaces `create_school_id_giga`)
6. Admin / disputed-region columns when lat/lon present
7. `connectivity_govt` ingestion timestamp when that column was uploaded
8. If `enable_connectivity` and not `LOCAL`: alias govt ID → `facility_id_govt` if needed, `get_country_rt_schools` → `merge_connectivity_to_master`
9. If `enable_registration_updates` and school + Gigameter source: `set_school_registration_verification_status`

| Step | Status |
| --- | --- |
| `map_govt_to_giga_columns` | Wired for all facilities |
| `add_connectivity_type_columns` | Wired (same params) |
| `create_school_id_giga` | Not restored; replaced by `STABLE_FACILITY_ID_CONTRACT` |
| `add_is_new_school` | Replaced by `add_is_new_entity` / `is_new_facility` |
| `merge_connectivity_to_master` / `get_country_rt_schools` | Wired; joins on `facility_id_govt` (legacy RT `school_id_*` renamed at merge) |
| `set_school_registration_verification_status` | Wired for schools only |
| `connectivity_govt` ingestion timestamp | Wired |
| Admin / disputed region | Wired |
| Write metadata Delta table | Restored as `pipeline_tables.facility_geolocation_metadata` (same upsert shape as school) |
| Post-approval → `school_master` / `school_reference` | Still out of scope; gold remains `facility_core` + extension |

### `merge_connectivity_to_master` rename note

The helper joins on `lower(facility_id_govt)`. Legacy RT rows that still expose `school_id_govt` / `school_id_giga` are renamed to `facility_id_*` inside the merge. School uploads whose govt column is still `school_id_govt` get a temporary `facility_id_govt` alias for the join, then that alias is dropped.

## `education_level_govt` note (health)

The facility DQ path no longer aliases columns into school check-engine names: the engines read `facility_level` / `facility_level_govt` directly. Do not reintroduce `education_level_govt` in the health registry schema.
