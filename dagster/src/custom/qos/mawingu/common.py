"""Mawingu (Zabbix) QoS fetch and transform logic — ports process_mawingu_raw.py and
aggregate_mawingu.py's prod path."""

from __future__ import annotations

from datetime import date

import pandas as pd
from src.custom.qos.mawingu.constants import ITEM_KEY_MAP, REFERENCE_SCHOOLS_FILEPATH
from src.custom.qos.zabbix_client import make_api_call, timestamp_to_unix
from src.settings import settings
from src.utils.adls import ADLSFileClient

from dagster import OpExecutionContext


def _custom_replace(value: str) -> str:
    if value in ITEM_KEY_MAP:
        return ITEM_KEY_MAP[value]
    if value.startswith("ubntStaSignal"):
        return "signal"
    return value


def fetch_silver_dataframe(
    target_date: date, adls_file_client: ADLSFileClient, context: OpExecutionContext
) -> pd.DataFrame:
    api_url = settings.MAWINGU_ZABBIX_API_URL
    token = settings.MAWINGU_ZABBIX_TOKEN

    host = make_api_call(api_url, token, "host.get")
    host = pd.DataFrame(host)

    schools_df = adls_file_client.download_csv_as_pandas_dataframe(REFERENCE_SCHOOLS_FILEPATH)
    schools_df["host_id"] = schools_df["host_id"].astype(str)
    filtered_host = list(schools_df["host_id"])

    item_params = {
        "output": ["itemid", "name", "key_", "hostid", "value_type", "status"],
        "hostids": list(host.hostid.unique()),
        "sortfield": "name",
    }
    items = make_api_call(api_url, token, "item.get", item_params)

    items_by_host: dict[str, list] = {}
    for item in items:
        items_by_host.setdefault(item["hostid"], []).append(item)

    start_date = timestamp_to_unix(f"{target_date.isoformat()} 00:00:00")
    end_date = timestamp_to_unix(f"{target_date.isoformat()} 23:59:59")

    all_history_data = []
    for school in filtered_host:
        for item in items_by_host.get(school, []):
            history_params = {
                "output": "extend",
                "history": item["value_type"],
                "itemids": item["itemid"],
                "time_from": start_date,
                "time_till": end_date,
                "sortfield": "clock",
                "sortorder": "ASC",
            }
            context.log.info(f"getting data for {school} {item['name']} {item['key_']}")
            history_data = pd.DataFrame(make_api_call(api_url, token, "history.get", history_params))
            history_data["key"] = item["key_"]
            history_data["name"] = item["name"]
            history_data["value_type"] = item["value_type"]
            history_data["hostid"] = school
            all_history_data.append(history_data)

    data = pd.concat(all_history_data, ignore_index=True) if all_history_data else pd.DataFrame()
    data = data[["hostid", "itemid", "name", "key", "value", "value_type", "clock", "ns"]]

    data["value"] = pd.to_numeric(data["value"])
    data["clock"] = pd.to_numeric(data["clock"])
    data["timestamp"] = pd.to_datetime(data["clock"], unit="s")
    data["value_type"] = data["value_type"].astype(int)
    data = data.rename(columns={"hostid": "host_id"})

    schools_df = schools_df[["host_id", "school_id_giga", "school_id_govt"]]
    ddf = pd.merge(data, schools_df, how="left", on="host_id")
    ddf = ddf[["timestamp", "host_id", "school_id_giga", "school_id_govt", "key", "value"]]

    ddf["key"] = ddf["key"].map(_custom_replace)
    ddf = ddf.sort_values(by=["timestamp", "host_id", "key"], ascending=False)

    wide_df = ddf.pivot_table(
        index=["timestamp", "host_id", "school_id_giga", "school_id_govt"],
        columns="key",
        values="value",
        aggfunc="first",
    ).reset_index()

    wide_df = wide_df[
        ~(wide_df["speed_download"].isna() & wide_df["speed_upload"].isna() & wide_df["latency"].isna())
    ]

    for stray_col in ["bandwidthTotal.[2]", "bandwidthTotal.[4]",
                       "net.if.speed[ifSpeed.2]", "net.if.status[ifOperStatus.2]",
                       "system.net.uptime[sysUpTime.0]"]:
        if stray_col in wide_df.columns:
            del wide_df[stray_col]

    wide_df["latency"] = wide_df["latency"].round(5) * 1000
    wide_df["speed_download"] = wide_df["speed_download"] / 1000000
    wide_df["speed_upload"] = wide_df["speed_upload"] / 1000000

    wide_df = wide_df.sort_values(by=["school_id_giga", "host_id", "timestamp"], ascending=False)
    wide_df["country_id"] = "KEN"
    wide_df["measurement_type"] = "usage"
    wide_df["provider"] = "Mawingu"
    return wide_df


def aggregate_gold_dataframe(silver_df: pd.DataFrame) -> pd.DataFrame:
    df = silver_df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["timestamp"] = df["timestamp"].dt.floor("H")

    ddf = df.groupby(["timestamp", "school_id_giga", "school_id_govt", "host_id"]).agg({
        "speed_download": ["mean", "max"],
        "speed_upload": ["mean", "max"],
        "latency": ["min", "mean", "max"],
        "signal": ["mean", "max"],
        "ping_status": [
            lambda x: x.notna().sum(),
            lambda x: (x == 1.0).sum(),
        ],
    }).reset_index()

    ddf.columns = [
        "timestamp", "school_id_giga", "school_id_govt", "host_id",
        "speed_download_mean", "speed_download_max",
        "speed_upload_mean", "speed_upload_max",
        "latency_min", "latency_mean", "latency_max",
        "signal_mean", "signal_max",
        "is_connected_all", "is_connected_true",
    ]

    ddf["country_id"] = "KEN"
    ddf["measurement_id"] = "KEN002"
    ddf["provider"] = "Mawingu"
    return ddf
