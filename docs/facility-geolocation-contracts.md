# Facility geolocation contracts

These are the external contracts for the health-facility pilot. A combined upload remains one record through DQ, staging, and approval; only approved records are projected into the shared core and facility-specific extension.

## Metaschema

Each metaschema row must provide `id`, `name`, `data_type`, and `is_nullable`; set `primary_key` on exactly one field. `is_important`, `is_system_generated`, `description`, `partition_order`, `license`, `units`, and `hint` remain supported metadata.

`health_facility_geolocation` is the combined ingestion metaschema. It owns the union of the health core-source and extension fields, uses `facility_id_giga` as its primary key, and drives upload mapping, type casts, staging, and the combined `health_facility_geolocation_silver` table. Approved silver rows are then projected into the two schemas below.

`facility_core` requires:

| Field | Type | Nullable | Contract |
| --- | --- | --- | --- |
| `facility_id_giga` | `string` | no | Primary key; deterministic from namespaced facility identity |
| `facility_id_govt` | `string` | yes | Preferred source identity; uniqueness is scoped by `facility_type` and `country_code` |
| `facility_type` | `string` | no | Controlled value; `health` for this pilot |
| `facility_name` | `string` | no | Shared display name |
| `facility_level` | `string` | yes | Shared level/classification |
| `country_code` | `string` | no | Uppercase ISO-3166 alpha-3 |
| `latitude` | `double` | no | WGS84, `[-90, 90]` |
| `longitude` | `double` | no | WGS84, `[-180, 180]` |
| `admin1` … `admin4` | `string` | yes | Derived administrative names |
| `admin1_id_giga` … `admin4_id_giga` | `string` | yes | Derived administrative identifiers |
| `disputed_region` | `string` | yes | Derived disputed-area name |
| `core_signature` | `string` | no | Hash of core-owned fields only |

`health_facility_details` requires:

| Field | Type | Nullable | Contract |
| --- | --- | --- | --- |
| `facility_id_giga` | `string` | no | Primary key and logical foreign key to `facility_core.facility_id_giga` |
| `facility_type_govt` | `string` | yes | Source-system facility classification |
| `ownership` | `string` | yes | Facility ownership category |
| `operator` | `string` | yes | Operating organization |
| `address` | `string` | yes | Facility address |
| `is_operational` | `boolean` | yes | Current operating status |
| `bed_count` | `integer` | yes | Available bed count |
| `staff_count` | `integer` | yes | Staff count |
| `services` | `string` | yes | Source service classification |
| `electricity_availability` | `string` | yes | Electricity availability |
| `water_availability` | `string` | yes | Water availability |
| `facility_data_source` | `string` | yes | Source organization/system |
| `facility_data_collection_year` | `integer` | yes | Source collection year |
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

Health configuration must use facility-facing names such as `dq_duplicate-facility_id_govt`, `dq_duplicate-facility_id_giga`, `dq_is_null_mandatory-facility_id_govt`, `dq_duplicate_set-facility_name_facility_level_location_id`, `dq_is_facility_density_greater_than_5`, and the existing coordinate/boundary check names. Historical school aliases are internal compatibility details and are not part of the health NocoDB contract. Health domains and ranges are supplied by `HEALTH_SPEC`; in particular, school education-level domains do not apply to `facility_level`.

## Ingestion portal contract

- Domain: `health-facility`; dataset: `geolocation`; approval label: `Health Facility Geolocation`.
- Upload path: `raw/uploads/health-facility-geolocation/<ISO3>/<upload_id>_<ISO3>_geolocation_<YYYYMMDD-HHMMSS>.<ext>`.
- `<upload_id>` is the `FileUpload.id`; ISO3 is uppercase; timestamp is UTC; supported tabular extensions follow the existing geolocation loader.
- `column_to_schema_mapping` maps uploaded headings to the combined core-plus-health schema. It must include mappings for `facility_id_govt`, `facility_name`, `latitude`, and `longitude`. Mapping targets must be unique.

The portal-created `FileUpload` row must set `id`, `created`, `uploader_id`, `uploader_email`, `country`, `dataset="geolocation"`, `source` (nullable), `original_filename`, `column_to_schema_mapping`, `column_license`, `metadata_json_path`, `dq_status`, `is_processed_in_staging=false`, and `approval_status`. Generated `filename` and `upload_path` must exactly match the blob above. Row counts, DQ paths, bronze path, and later status fields are pipeline-owned.

Approval JSON is written at the agreed approval-sensor path and has this shape:

```json
{
  "upload_id": "<FileUpload.id>",
  "approved_change_ids": ["<staging change_id>"],
  "rejected_change_ids": ["<staging change_id>"],
  "approval_request_log_id": "<audit-log id>"
}
```

`upload_id` and `approval_request_log_id` are required. Approved and rejected IDs must be disjoint and belong to the upload; `__all__` is a sentinel, not a real ID, and must not be mixed with explicit approved IDs. `ApprovalRequest` is unique on `(country, dataset)` and uses dataset label `Health Facility Geolocation`.

## Publishing and reconciliation

For each approval, merge `facility_core` first, then `health_facility_details`, using `facility_id_giga` and the table-specific signature. Both merges and deletes must be deterministic and idempotent. Mark staging changes `PROCESSED` and reset approval state only after both writes succeed. `FileUpload.is_processed_in_staging` retains its existing meaning: the upload has successfully entered staging, not that approval publishing is complete.

Because the writes are not transactional, retries must complete a missing extension write without duplicating or rewriting an unchanged core row. Reconciliation must detect and fail on orphan extensions, missing required extensions, facility-type mismatches, duplicate IDs, signature mismatches, and staging rows marked processed before both projections exist. No foreign-key cascade is assumed; the publish-complete asset is the boundary after which both projections are guaranteed to agree.

## Out of scope

Migration, backfill, replacement, or cut-over of `school_master` (and its current school geolocation paths, assets, jobs, sensors, tables, coverage cascades, or release behavior) is explicitly out of scope.
