# QoS Migration Setup (BRA / ZAF / KEN / MNG)

This covers the manual setup required before the Brazil, Isizwe (ZAF), Mawingu (KEN),
and Mongolia (MNG) QoS assets (`src/assets/qos/{bra,isizwe,mawingu,mongolia}`) can run.
Code and wiring are done; the items below are external state nothing in this repo can
provision.

## 1. Secrets and reference data

See [QoS Assets Overview](qos_assets_overview.md#secrets) for the full secrets
table (6 new `src/settings.py` values, wired through Azure Pipelines but not
yet given real values per environment) and the
[reference data table](qos_assets_overview.md#reference-data-on-adls) (2 CSVs
that need to be copied from the VM to ADLS and kept in sync).

## 2. Needs verification before relying on it

- **`bra_qos_raw_republish`'s table name.** It reads `qos.bra` (see
  `src/assets/qos/bra/qos_bra_raw_republish.py`), inferred from the
  `school_master.<iso3>` naming convention extended to the generic QoS ingestion
  sensor's target tables. This hasn't been checked against the live metastore.
- **ADLS storage account/container match.** These assets write to `gold/qos/<ISO3>` and
  `gold/qos-raw/<ISO3>` via `ADLSFileClient`, which resolves to whatever
  `AZURE_STORAGE_ACCOUNT_NAME`/`AZURE_BLOB_CONTAINER_NAME` are set to per environment.
  Confirm this is the same storage the existing `school_qos__gold_csv_to_deltatable_sensor`
  / `school_qos_raw__gold_csv_to_deltatable_sensor` (`src/sensors/adhoc.py`) watch, so
  published files actually get picked up and land in `qos.<iso3>` / `qos_raw.<iso3>`.
- **Never run.** This was built and lint/syntax-checked only (`ruff`, `py_compile`) - no
  Dagster/Spark/ADLS environment was available to actually execute an asset.
