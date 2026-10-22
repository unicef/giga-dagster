# URY (Ceibal) Zabbix retest Job

A one-off, read-only check of the Ceibal Zabbix API. It runs through the Giga VPN proxy (`giga-vpn-proxy.ictd-ooi-giga-devops-dev:8080`, non-prod cluster). It is a first step before a `qos/ury` Dagster asset (same pattern as `mawingu`).

It checks:
- the API version and login
- the GIGA host group (`1199`)
- the 4 target items (ping_check, Latencia, WAN in, WAN out) for each router template
- that `history.get` returns data when called with `"history": <value_type>` (Ceibal's fix, 1 Jul 2026)
- with `--codicen`: how many Codicens have more than one host (counts only)

There is a 2 s pause between calls and every call has a `limit`, because Ceibal's server is not tuned for API load.

| File | Purpose |
|---|---|
| `Dockerfile` | `python:3.12-slim` image that runs as a non-root user |
| `job.yaml` | Job in `ictd-ooi-data-analysis-dev`: no retries, 15 min deadline, cleaned up after 24 h |
| `kustomization.yaml` | Sets the namespace and image (pipeline: `kustomize edit set image …`, then `kubectl apply -k`). Delete the previous Job before re-applying, because Job templates are immutable |
| `retest_connection.py`, `zabbix_client.py` | The checks and a Zabbix 5.0–7.x JSON-RPC client |

**Credentials:** read from K8s Secret `ceibal-zabbix-secrets`, which DevOps syncs from Azure Key Vault. Keys: `ZABBIX_USER`, `ZABBIX_PASSWORD`, and optionally `ZABBIX_PROXY_USER` / `ZABBIX_PROXY_PASS`.

**Result:** `kubectl logs job/ury-zabbix-retest -n ictd-ooi-data-analysis-dev`. The SUMMARY block contains no hostnames or credentials.
