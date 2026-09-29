# QoS Migration Setup (BRA / ZAF / KEN / MNG)

This covers the manual setup required before the Brazil, Isizwe (ZAF), Mawingu (KEN),
and Mongolia (MNG) QoS assets (`src/assets/qos/{bra,isizwe,mawingu,mongolia}`) can run.
Code and wiring are done; the items below are external state nothing in this repo can
provision.

## 1. Secrets

Six new settings were added to `src/settings.py`, wired through
`azure/templates/variables.yaml` and `azure/templates/create-config.yaml` into the
`giga-dagster-secrets` Kubernetes Secret, the same way `MONGOLIA_API_URL` already is.
What's still needed: the actual values, set as pipeline variables in the Azure DevOps
variable group for each environment (dev/stg/prd) that feeds this deployment.

| Variable | Source (from qos_scripts on the VM) |
|---|---|
| `BRAZIL_API_URL` | `bra_scripts/get_qos_brazil.py`'s `url` |
| `ISIZWE_ZABBIX_API_URL` | `isizwe_scripts/process_isizwe_raw.py`'s `ZABBIX_API_URL` |
| `ISIZWE_ZABBIX_TOKEN` | `isizwe_scripts/process_isizwe_raw.py`'s `token` |
| `MAWINGU_ZABBIX_API_URL` | `mawingu_scripts/process_mawingu_raw.py`'s `ZABBIX_API_URL` |
| `MAWINGU_ZABBIX_TOKEN` | `mawingu_scripts/process_mawingu_raw.py`'s `token` |
| `MONGOLIA_DEVICE_BEARER_TOKEN` | `mng_scripts/get_bandwidth_utilization.py`'s `headers["Authorization"]` bearer token |

Isizwe and Mawingu use different Zabbix instances/tokens today (confirm this is still
true rather than assuming one shared Zabbix deployment) - each gets its own pair of
variables above rather than a shared one.

## 2. Reference data on ADLS

Three assets need device/school reference data that has no Spark table equivalent and
must be uploaded to ADLS as a CSV, at the paths below. These already exist on the VM;
they need to be copied over and then kept in sync going forward (nothing in this repo
does that automatically).

| ADLS path | Source file on the VM | Used by |
|---|---|---|
| `reference/qos/KEN/mawingu_schools.csv` | `/home/azureuser/mawingu/mawingu_schools.csv` | `mawingu_qos` (host_id → school_id_giga/school_id_govt) |
| `reference/qos/MNG/whitelisted_devices.csv` | `/home/azureuser/mongolia/whitelisted_devices.csv` | `mongolia_qos_raw_json` (device_id → fetch_url) and `mongolia_qos_gold` (device_id → school_id_govt + device metadata) |

Isizwe needs no reference file - Zabbix's `host.get` response already carries
`school_id_govt` as a host tag.

Mongolia's `MNG_school_geolocation_coverage_master.csv` (school_id_govt → school_id_giga)
was **not** ported as a reference file - the new code reads `school_master.mng` directly
instead, since that mapping already exists as a Spark table. Confirm that table has full
coverage before the first real run; if it's missing rows the VM's CSV had, some schools
will silently drop out at the `school_id_giga` null-filter step.

## 3. Needs verification before relying on it

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
