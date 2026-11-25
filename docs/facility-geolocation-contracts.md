# Facility geolocation contracts

These are the external contracts for facility geolocation (school and health). A combined upload remains one record through DQ, staging, and approval; only approved records are projected into the shared core and facility-specific extension. Raw uploads land under `raw/uploads/facility-geolocation/<ISO3>/<schema_key>/` and are routed by `schema_key`.

See also:

- [facility-cutover-school-master-and-connectivity.md](./facility-cutover-school-master-and-connectivity.md) — what still writes `school_master`, connectivity RT hard-wiring, and bronze steps dropped in the factory cutover
- [facility-schema-rename-and-nocodb-dq.md](./facility-schema-rename-and-nocodb-dq.md) — `facility_*` / `geo_*` rename and NocoDB DQ CSV notes

## Metaschema

Each metaschema row must provide `id`, `name`, `data_type`, and `is_nullable`; set `primary_key` on exactly one field. `is_important`, `is_system_generated`, `description`, `column_group`, `license`, `units`, and `hint` remain supported metadata.

`health_facility_geolocation` is the combined ingestion metaschema. It owns the union of the health core-source and extension fields, uses `facility_id_giga` as its primary key, and drives upload mapping, type casts, staging, and the combined `health_facility_geolocation_silver` table. Approved silver rows are then projected into the two schemas below.

`facility_core` requires shared identity/geo fields (upload + pipeline-owned):

| Field | Type | Nullable | Contract |
| --- | --- | --- | --- |
| `facility_id_giga` | `string` | no | Primary key; deterministic from namespaced facility identity |
| `facility_id_govt` | `string` | yes | Preferred source identity; uniqueness is scoped by `facility_type` and `country_code` |
| `facility_id_type_govt` | `string` | yes | Registry that issued `facility_id_govt` |
| `facility_type` | `string` | no | Controlled value (`school` / `health`) |
| `facility_name` | `string` | no | Shared display name |
| `facility_level_govt` | `string` | yes | Government level/classification |
| `facility_level` | `string` | yes | Giga-normalized level derived from `facility_level_govt` |
| `country_code` | `string` | no | Uppercase ISO-3166 alpha-3 |
| `geo_latitude` | `double` | no | WGS84, `[-90, 90]` |
| `geo_longitude` | `double` | no | WGS84, `[-180, 180]` |
| `geo_admin1_govt` / `geo_admin2_govt` | `string` | yes | Government admin divisions |
| `admin1` … `admin4` | `string` | yes | Derived administrative names |
| `admin1_id_giga` … `admin4_id_giga` | `string` | yes | Derived administrative identifiers |
| `disputed_region` | `string` | yes | Derived disputed-area name |
| `core_signature` | `string` | no | Hash of core-owned fields only |

Core also owns the shared upload profile fields from the facilities core schema (ownership, connectivity, electricity, devices, staff, etc.).

`school_facility_details` extension owns: `computer_lab_availability`, `students_num`, `students_female_num`, `students_male_num`, `students_other_num`, plus `extension_signature`.

`health_facility_details` requires:

| Field | Type | Nullable | Contract |
| --- | --- | --- | --- |
| `facility_id_giga` | `string` | no | Primary key and logical foreign key to `facility_core.facility_id_giga` |
| `facility_id_2_govt` / `facility_id_2_type_govt` | `string` | yes | Secondary government identifier |
| `facility_health_type_govt` | `string` | yes | Health facility type (hospital, clinic, …) |
| `facility_health_care_type` | `string` | yes | Inpatient / outpatient / both |
| `facility_health_service_type` | `string` | yes | Clinical service areas |
| `system_hmis` / `system_hmis_used` | `string` | yes | HMIS usage and system name |
| `system_emr` / `system_emr_used` | `string` | yes | EMR usage and system name |
| `outpatients_avg` / `outpatients_ref_period` | mixed | yes | Outpatient volume |
| `inpatients_avg` / `inpatients_ref_period` | mixed | yes | Inpatient volume |
| `pop_catchment_govt` | `integer` | yes | Official catchment population |
| `extension_signature` | `string` | no | Hash of health-extension-owned fields only |

Health-specific fields must not duplicate core-owned fields. There is exactly one core row and at most one health extension row per `facility_id_giga`; Delta/Hive does not enforce the foreign key, so publishing and reconciliation must.

## NocoDB DQ contract

Create a health-specific DQ table and register its name-to-ID mapping; do not reuse `SchoolGeolocationMasterDQChecks`. Every configured check row must contain:

- `DQ Table Column Name`: unique emitted `dq_*` column.
- `DQ Check Category`, `Human Readable Name`, and `UI Error Description`: report grouping and reviewer-facing labels.
- `Column Checked`: one source column or a comma-separated set.
- `Create DQ` and `Update DQ`: `always`, `if in file`, or disabled.
- `Critical For`: `always`, `create_only`, or non-critical.
- `DQR Warnings`: `Yes` only when the check contributes to warning counts.
- `Related Check ID`: stable ordering/grouping key.

Core identity, coordinate, country/boundary, duplicate, density, and geospatial checks use core fields and compare records within `facility_type`. Health-only checks use extension fields. A referenced `dq_*` column must exist in pipeline output, labels must be non-empty, and configuration changes must be validated before enabling the sensor.

School and health NocoDB tables (`SchoolGeolocationMasterDQChecks`, `HealthFacilityGeolocationDQChecks`) must use facility-facing names such as `dq_duplicate-facility_id_govt`, `dq_is_null_mandatory-facility_id_govt`, `dq_precision-geo_latitude`, `dq_duplicate_set-facility_name_facility_level_location_id`, and `dq_is_facility_density_greater_than_5`. Both tables include a Giga-normalized `facility_level` domain check. The shared check engines emit these names directly; there is no internal school/`latitude` alias layer.

## Ingestion portal contract

School and health facility geolocation share one raw upload root. Sensors route by `schema_key`:

- Upload path: `raw/uploads/facility-geolocation/<ISO3>/<schema_key>/<upload_id>_<ISO3>_geolocation_<YYYYMMDD-HHMMSS>.<ext>`.
- School `schema_key`: `schools` (approval label: `School Geolocation`).
- Health `schema_key`: `health_facilities` (domain: `health-facility`; approval label: `Health Facility Geolocation`).
- Dataset stays `geolocation` for both. `<upload_id>` is the `FileUpload.id`; ISO3 is uppercase; timestamp is UTC; supported tabular extensions follow the existing geolocation loader.
- `column_to_schema_mapping` maps uploaded headings to the combined core-plus-member schema. Both school and health mappings must use `facility_id_govt`, `facility_name`, `geo_latitude`, and `geo_longitude` (plus `facility_level_govt` when present). Mapping targets must be unique.

The portal-created `FileUpload` row must set `id`, `created`, `uploader_id`, `uploader_email`, `country`, `dataset="geolocation"`, `schema_key`, schema version pins, `source` (nullable), `original_filename`, `column_to_schema_mapping`, `column_license`, `metadata_json_path`, `dq_status`, `is_processed_in_staging=false`, and `approval_status`. Generated `filename` and `upload_path` must exactly match the blob above. Row counts, DQ paths, bronze path, and later status fields are pipeline-owned.

Approval JSON is written at the agreed approval-sensor path and has this shape:

```json
{
  "upload_id": "<FileUpload.id>",
  "approved_change_ids": ["<staging change_id>"],
  "rejected_change_ids": ["<staging change_id>"],
  "approval_request_log_id": "<audit-log id>"
}
```

`upload_id` is required. `approval_request_log_id` is required when any row is approved; a reject-only file may omit it. Approved and rejected IDs must be disjoint and belong to the upload; `__all__` is a sentinel, not a real ID, and must not be mixed with explicit approved IDs. `ApprovalRequest` is unique on `(country, dataset)` and uses dataset label `Health Facility Geolocation`.

## Staging and error tables

Each facility type has one staging table and one DQ error table shared by all countries, in the `pipeline_tables` schema. Rows are distinguished by `country_code` (uppercase ISO3) and both tables are partitioned by it:

| | School | Health |
| --- | --- | --- |
| Staging | `pipeline_tables.school_facility_geolocation_staging` | `pipeline_tables.health_facility_geolocation_staging` |
| DQ errors | `pipeline_tables.school_facility_geolocation_error_table` | `pipeline_tables.health_facility_geolocation_error_table` |

Every read, update, and delete against these tables must filter on `country_code` as well as `upload_id`; this includes the portal approval queries. Staging rows keep the existing contract (`change_type`, `upload_id`, `uploaded_columns`, `status`, `change_id` = `facility_id_giga|upload_id|change_type`, `created_at`, `processed_at`, `approval_request_log_id`). Error rows carry the failed DQ row plus `upload_id`, `country_code`, and `created_at`; re-running an upload replaces its rows. Silver tables stay one table per country.

The legacy per-country `school_geolocation_staging.<iso3>` and `school_geolocation_error_table.<iso3>` belong to the old school pipeline and are not written by the facility pipeline.

## Publishing and reconciliation

For each approval, merge `facility_core` first, then the facility-specific extension (`school_facility_details` or `health_facility_details`), using `facility_id_giga` and the table-specific signature. Both merges and deletes must be deterministic and idempotent. Mark staging changes `PROCESSED` and reset approval state only after both writes succeed. `FileUpload.is_processed_in_staging` retains its existing meaning: the upload has successfully entered staging, not that approval publishing is complete.

Because the writes are not transactional, retries must complete a missing extension write without duplicating or rewriting an unchanged core row. Reconciliation must detect and fail on orphan extensions, missing required extensions, facility-type mismatches, duplicate IDs, signature mismatches, and staging rows marked processed before both projections exist. No foreign-key cascade is assumed; the publish-complete asset is the boundary after which both projections are guaranteed to agree.

School geolocation uploads no longer publish to `school_master` / `school_reference`; gold is `facility_core` + `school_facility_details`. Coverage and other pipelines that still target `school_master` are unchanged.

## Out of scope

Backfill or dual-write from historical `school_master` into `facility_core`, and migrating coverage / connectivity / QoS onto the facility core model, remain out of scope.
