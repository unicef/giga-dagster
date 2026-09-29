# QoS Assets Overview (BRA / ZAF / KEN / MNG)

What each QoS asset added in the `qos-scripts-migration` branch does, which
`qos_scripts` VM cron script(s) it replaces, and what consumes its output
downstream. See [QoS Migration Setup](qos_migration_setup.md) for the secrets
and reference data these need before they can run.

## BRA

### `bra_qos`
*Partitioned daily (`DailyPartitionsDefinition`), targets today's own
still-accumulating partition, scheduled 6×/day.*

Fetches one day's speed-test records from nic.br's API, joins to
`school_master.bra`, splits into silver (matched to a school) / gold (silver +
IPv4-only) / error rows, and uploads bronze JSON + silver + gold + error
parquet to ADLS.

- **Replaces:** `get_qos_brazil.py`, `backfill_qos_brazil.py`
- **Downstream:** `gold/qos/BRA/*.parquet` is picked up by the existing
  generic `school_qos__gold_csv_to_deltatable_sensor`, which publishes it into
  the `qos.bra` Delta table (computing `signature`/`gigasync_id`/`date` along
  the way).

### `bra_qos_raw_republish`
*Partitioned daily, targets yesterday.*

Reads yesterday's rows back out of `qos.bra`, pads in the device-level columns
BRA never produces (`inbound_traffic`, `outbound_traffic`, `in_perc`,
`out_perc`, `device_id`) as null/empty, and republishes to
`gold/qos-raw/BRA/`. BRA ingests pre-aggregated speed-test data, not
per-device polling, so it has no real "raw" data - this exists purely so
`qos_raw.bra` isn't empty for whatever expects it to exist alongside every
other country's genuine raw table.

- **Replaces:** `update_qos_brazil.py`
- **Downstream:** `gold/qos-raw/BRA/*.parquet` feeds the generic
  `school_qos_raw__gold_csv_to_deltatable_sensor`, which publishes it into
  `qos_raw.bra`.

## ZAF (Isizwe)

### `isizwe_qos`
*Partitioned daily, targets yesterday.*

Pulls one day's Zabbix history for every tagged host, pivots it wide, joins to
`school_master.zaf`, uploads silver parquet, then aggregates hourly and
uploads gold parquet.

- **Replaces:** `process_isizwe_raw.py` + `aggregate_isizwe.py` + their two
  backfill scripts. One asset now does what took two chained cron scripts -
  the original split only existed to hand a file between two processes, which
  Dagster doesn't need.
- **Downstream:** `gold/qos/ZAF/*.parquet` feeds the same generic sensor as
  BRA, publishing into `qos.zaf`.

## KEN (Mawingu)

### `mawingu_qos`
*Partitioned daily, targets yesterday.*

Same shape as Isizwe, but school mapping comes from `mawingu_schools.csv` on
ADLS (Mawingu's Zabbix hosts don't carry school tags the way Isizwe's do), and
the gold schema is an 18-column subset of the 28 non-downstream PRD columns.
The other 10 (`ce_egress`, `ce_ingress`, `pe_egress`, `pe_ingress`, `latency`,
`latency_probe`, `speed_download`, `speed_download_probe`, `speed_upload`,
`speed_upload_probe`) were never produced by any script in `qos_scripts` -
`process_mawingu_clean.py` is commented out of the live crontab and was never
active in prod.

- **Replaces:** `process_mawingu_raw.py` + `aggregate_mawingu.py` + their
  backfills.
- **Downstream:** `gold/qos/KEN/*.parquet` feeds `qos.ken`.

## MNG

### `mongolia_qos_raw_json`
*Not partitioned - runs every 5 minutes.*

Polls every device in `whitelisted_devices.csv` (ADLS reference file) and
lands each raw JSON response at `raw/qos/MNG/<date>/...`.

- **Replaces:** `get_bandwidth_utilization.py`
- **Downstream:** nothing reads this directly except `mongolia_qos_gold` the
  next day - it's pure landing/accumulation, never published to `qos`/`qos_raw`
  itself. There's nothing to backfill for a missed poll, so this asset has no
  `partitions_def`.

### `mongolia_qos_gold`
*Partitioned daily, targets yesterday.*

Lists and parses that day's raw JSON snapshots, cleans/merges them (school
mapping now comes from `school_master.mng` instead of the VM's
`MNG_school_geolocation_coverage_master.csv`), uploads `raw_clean` to
`gold/qos-raw/MNG/`, then aggregates hourly and uploads to `gold/qos/MNG/`.

- **Replaces:** `process_mongolia_raw.py` + `process_mongolia_clean.py` +
  their backfills. Only models the working prod path -
  `process_bandwidth_utilization.py`'s independent, less-complete
  staging-only pipeline (missing `min`/`count` aggregates, empty `provider`,
  no `measurement_type`) was deliberately not ported.
- **Downstream:** feeds `qos_raw.mng` and `qos.mng` via the two generic
  sensors. Depends on `mongolia_qos_raw_json` having actually polled the
  target day - a partition backfilled for a date with no raw snapshots on
  ADLS just produces zero rows, matching `backfill_mongolia_raw.py`'s
  original WARNING-and-skip behavior.

## Shared modules (not assets)

- `custom/qos/schema_utils.py` - the cast/serialize helpers (`enforce_schema`,
  `to_parquet_bytes`) every country's `schema.py` uses.
- `custom/qos/zabbix_client.py` - the JSON-RPC client Isizwe and Mawingu
  share.

## Secrets

Added to `src/settings.py`, wired through `azure/templates/variables.yaml` and
`azure/templates/create-config.yaml` into the `giga-dagster-secrets`
Kubernetes Secret, the same way `MONGOLIA_API_URL` already is. Values still
need to be set as pipeline variables in the Azure DevOps variable group for
each environment (dev/stg/prd) - nothing in this repo can provision them.

| Variable | Used by | Source (from qos_scripts on the VM) |
|---|---|---|
| `BRAZIL_API_URL` | `bra_qos` | `bra_scripts/get_qos_brazil.py`'s `url` |
| `ISIZWE_ZABBIX_API_URL` | `isizwe_qos` | `isizwe_scripts/process_isizwe_raw.py`'s `ZABBIX_API_URL` |
| `ISIZWE_ZABBIX_TOKEN` | `isizwe_qos` | `isizwe_scripts/process_isizwe_raw.py`'s `token` |
| `MAWINGU_ZABBIX_API_URL` | `mawingu_qos` | `mawingu_scripts/process_mawingu_raw.py`'s `ZABBIX_API_URL` |
| `MAWINGU_ZABBIX_TOKEN` | `mawingu_qos` | `mawingu_scripts/process_mawingu_raw.py`'s `token` |
| `MONGOLIA_DEVICE_BEARER_TOKEN` | `mongolia_qos_raw_json` | `mng_scripts/get_bandwidth_utilization.py`'s `headers["Authorization"]` bearer token |

Isizwe and Mawingu use different Zabbix instances/tokens today (confirm this
is still true rather than assuming one shared Zabbix deployment) - each gets
its own pair of variables rather than a shared one. `bra_qos_raw_republish`
and `mongolia_qos_gold` need no secret of their own; they only read data
their sibling asset already landed.

## Reference data on ADLS

Two assets need device/school reference data that has no Spark table
equivalent and must be uploaded to ADLS as a CSV at the paths below. These
already exist on the VM; they need to be copied over and then kept in sync
going forward (nothing in this repo does that automatically).

| ADLS path | Used by | Source file on the VM | Maps |
|---|---|---|---|
| `reference/qos/KEN/mawingu_schools.csv` | `mawingu_qos` | `/home/azureuser/mawingu/mawingu_schools.csv` | `host_id` → `school_id_giga`/`school_id_govt` |
| `reference/qos/MNG/whitelisted_devices.csv` | `mongolia_qos_raw_json`, `mongolia_qos_gold` | `/home/azureuser/mongolia/whitelisted_devices.csv` | `device_id` → `fetch_url` (raw_json) and → `school_id_govt` + device metadata (gold) |

Isizwe needs no reference file - Zabbix's `host.get` response already carries
`school_id_govt` as a host tag, so `isizwe_qos` only needs `school_master.zaf`.

Mongolia's old `MNG_school_geolocation_coverage_master.csv`
(`school_id_govt` → `school_id_giga`) was **not** ported as a reference file -
`mongolia_qos_gold` reads `school_master.mng` directly instead, since that
mapping already exists as a Spark table. Confirm that table has full coverage
before the first real run; if it's missing rows the VM's CSV had, those
schools will silently drop out at the `school_id_giga` null-filter step.

## Backfilling

Each partitioned asset takes its target date from `context.partition_key`
instead of computing it from `datetime.now()`, so backfilling a past date is
a normal Dagster partition backfill (UI or CLI) rather than running a
bespoke `--start-date`/`--end-date` script. `bra_qos` is the one exception in
spirit, not mechanism: because it's re-polled 6×/day for the *same*
still-open day, its schedule is a custom `@schedule` that always targets
today's partition, rather than `build_schedule_from_partitioned_job` (used
for the other four), which targets the most recently *completed* partition.
