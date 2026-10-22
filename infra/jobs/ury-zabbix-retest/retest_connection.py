#!/usr/bin/env python3
"""
URY (Ceibal) Zabbix retest: read-only checks to run from a pod in the Giga
dev cluster (namespace ictd-ooi-data-analysis-dev), where the VPN proxy resolves.

Checks:
  1. API version (Ceibal planned an upgrade ~July 2026; may no longer be 5.0.20)
  2. Login + GIGA host group id (expected 1199)
  3. Per template: the 4 target items (name, key_, value_type, units, preprocessing)
  4. history.get on the item ids sent to Ceibal — default vs "history": <value_type>
     (Facundo, 1 Jul 2026: items are float, so history.get needs "history": 0)
  5. --codicen (optional): duplicate Codicen count. Prints counts + Codicens only,
     never full hostnames (Ceibal privacy rule).

Every call has a limit and a pause between calls (Ceibal server is not
optimised for API queries).

Env:
  ZABBIX_URL          default http://giga-vpn-proxy.ictd-ooi-giga-devops-dev:8080
  ZABBIX_USER         default app_giga
  ZABBIX_PASSWORD     required (type it with `read -s`, don't put it in the pod spec)
  ZABBIX_PROXY_USER / ZABBIX_PROXY_PASS   optional proxy basic auth

Usage:
  python retest_connection.py [--codicen]
"""

import argparse
import os
import sys
import time
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from zabbix_client import ZabbixClient

GROUP_NAME = "GIGA"
EXPECTED_GROUPID = "1199"
TEMPLATES = ["Cisco_Router_ISR11001", "Cisco_Router_3G", "Cisco_Router891"]
TARGET_ITEMS = ["ping_check", "Latencia", "WAN in", "WAN out"]
# Item ids sent to Facundo on 11 Jun 2026 (history.get returned NULL)
CEIBAL_ITEMIDS = ["2342500", "2342516", "2342517", "2342499"]
VALUE_TYPES = {"0": "float", "1": "char", "2": "log", "3": "uint", "4": "text"}
PAUSE_S = 2

results = []  # (check, ok, detail)


def record(check, ok, detail):
    results.append((check, ok, detail))
    print(f"  [{'OK' if ok else 'FAIL'}] {check}: {detail}")


def call(client, method, params):
    out = client.call(method, params)
    time.sleep(PAUSE_S)
    return out


def check_version(client):
    print("\n1. API version")
    version = client.get_version()
    changed = version != "5.0.20"
    note = "CHANGED since Apr 2026 (was 5.0.20)" if changed else "unchanged"
    record("version", True, f"{version} ({note})")


def check_login_and_group(client):
    print("\n2. Login + host group")
    client.login()
    login_key = "username" if client._version_tuple >= (5, 4) else "user"
    auth_mode = "Bearer header" if client._version_tuple >= (7, 2) else "body auth"
    record("login", True, f'user.login param "{login_key}", {auth_mode}')

    groups = call(
        client,
        "hostgroup.get",
        {
            "output": ["groupid", "name"],
            "filter": {"name": [GROUP_NAME]},
            "limit": 1,
        },
    )
    gid = groups[0]["groupid"] if groups else None
    record(
        "GIGA groupid", gid == EXPECTED_GROUPID, f"{gid} (expected {EXPECTED_GROUPID})"
    )
    return gid


def check_templates(client):
    print("\n3. Target items per template")
    templates = call(
        client,
        "template.get",
        {
            "output": ["templateid", "host"],
            "filter": {"host": TEMPLATES},
            "limit": len(TEMPLATES),
        },
    )
    found = {t["host"]: t["templateid"] for t in templates}
    missing = [t for t in TEMPLATES if t not in found]
    record(
        "templates found",
        not missing,
        f"{len(found)}/{len(TEMPLATES)}" + (f", missing {missing}" if missing else ""),
    )

    for name, tid in found.items():
        items = call(
            client,
            "item.get",
            {
                "output": ["itemid", "name", "key_", "value_type", "units"],
                "templateids": [tid],
                "filter": {"name": TARGET_ITEMS},
                "selectPreprocessing": "extend",
                "limit": 20,
            },
        )
        names = {i["name"] for i in items}
        absent = [n for n in TARGET_ITEMS if n not in names]
        record(
            f"{name} items",
            not absent,
            f"{len(names)}/4" + (f", missing {absent}" if absent else ""),
        )
        for i in sorted(items, key=lambda x: x["name"]):
            steps = [str(p.get("type")) for p in i.get("preprocessing", [])]
            print(
                f"      {i['name']:<11} key_={i['key_']:<28} "
                f"value_type={VALUE_TYPES.get(i['value_type'], i['value_type'])} "
                f"units={i['units']!r} preprocessing_types={steps}"
            )


def check_history(client):
    print("\n4. history.get on the item ids sent to Ceibal (last 24 h)")
    items = call(
        client,
        "item.get",
        {
            "output": ["itemid", "name", "value_type", "units", "lastclock"],
            "itemids": CEIBAL_ITEMIDS,
            "limit": len(CEIBAL_ITEMIDS),
        },
    )
    if not items:
        record(
            "items visible",
            False,
            "none of the 4 item ids returned (permissions or ids changed?)",
        )
        return

    now = int(time.time())
    window = {"time_from": now - 86400, "time_till": now}
    for i in items:
        base = {
            "output": "extend",
            "itemids": [i["itemid"]],
            "sortfield": "clock",
            "sortorder": "DESC",
            "limit": 10,
            **window,
        }
        default_rows = call(client, "history.get", base)
        typed_rows = call(
            client, "history.get", {**base, "history": int(i["value_type"])}
        )
        vt = VALUE_TYPES.get(i["value_type"], i["value_type"])
        latest = typed_rows[0]["value"] if typed_rows else "-"
        record(
            f"history {i['itemid']} ({i['name']})",
            bool(typed_rows),
            f"default={len(default_rows)} rows, history={i['value_type']} ({vt})={len(typed_rows)} rows, "
            f"latest={latest} {i['units']}",
        )


def check_codicen(client, gid):
    print("\n5. Codicen duplicates (counts only)")
    hosts = call(
        client,
        "host.get",
        {
            "output": ["host"],
            "groupids": [gid],
            "filter": {"status": "0"},
            "limit": 5000,
        },
    )
    codicens = Counter(h["host"].split("_")[0] for h in hosts)
    non_numeric = [c for c in codicens if not c.isdigit()]
    dupes = {c: n for c, n in codicens.items() if n > 1}
    record(
        "hosts", True, f"{len(hosts)} monitored hosts, {len(codicens)} unique Codicens"
    )
    record("non-numeric Codicens", not non_numeric, f"{len(non_numeric)}")
    record(
        "Codicens with >1 host",
        True,
        f"{len(dupes)}; examples: {dict(list(dupes.items())[:10])}",
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--codicen", action="store_true", help="also count duplicate Codicens"
    )
    args = ap.parse_args()

    password = os.environ.get("ZABBIX_PASSWORD", "").strip()
    if not password:
        sys.exit(
            "ZABBIX_PASSWORD is required (use: read -s ZABBIX_PASSWORD; export ZABBIX_PASSWORD)"
        )
    proxy_user = os.environ.get("ZABBIX_PROXY_USER", "").strip()
    proxy_pass = os.environ.get("ZABBIX_PROXY_PASS", "").strip()

    client = ZabbixClient(
        url=os.environ.get(
            "ZABBIX_URL", "http://giga-vpn-proxy.ictd-ooi-giga-devops-dev:8080"
        ).strip(),
        username=os.environ.get("ZABBIX_USER", "app_giga").strip(),
        password=password,
        http_auth=(proxy_user, proxy_pass) if proxy_user else None,
    )
    print(f"Target: {client.api_url}")

    try:
        check_version(client)
        gid = check_login_and_group(client)
        for step in (check_templates, check_history):
            try:
                step(client)
            except Exception as e:
                record(step.__name__, False, str(e))
        if args.codicen and gid:
            try:
                check_codicen(client, gid)
            except Exception as e:
                record("check_codicen", False, str(e))
    except Exception as e:
        record("connection", False, str(e))
    finally:
        try:
            client.logout()
        except Exception:
            pass

    print(
        "\n"
        + "=" * 60
        + "\nSUMMARY (safe to paste: no hostnames or credentials)\n"
        + "=" * 60
    )
    for check, ok, detail in results:
        print(f"{'PASS' if ok else 'FAIL'}  {check}: {detail}")
    sys.exit(0 if all(ok for _, ok, _ in results) else 1)


if __name__ == "__main__":
    main()
