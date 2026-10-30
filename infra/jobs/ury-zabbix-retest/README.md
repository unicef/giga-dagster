# URY (Ceibal) Zabbix retest Job

One-off, read-only check of Ceibal's Zabbix API through the Giga VPN proxy, before building a `qos/ury` asset.

It checks the API version, login, the GIGA host group (`1199`), the 4 target items per router template, `history.get` with `"history": <value_type>`, and duplicate Codicens (counts only). Every call has a `limit` and a 2 s pause.

**Run:** queue `azure/azure-pipelines-ury-retest.yaml` (manual). It builds the image, deletes the previous Job and applies this one in `ictd-ooi-data-analysis-dev`.

**Credentials:** Secret `ceibal-zabbix-secrets` (`ZABBIX_USER`, `ZABBIX_PASSWORD`, optional `ZABBIX_PROXY_USER` / `ZABBIX_PROXY_PASS`).

**Result:** `kubectl logs job/ury-zabbix-retest -n ictd-ooi-data-analysis-dev`. The SUMMARY block has no hostnames or credentials.
