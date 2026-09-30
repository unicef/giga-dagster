"""Isizwe (Zabbix) QoS fetch and transform logic — ports process_isizwe_raw.py and
aggregate_isizwe.py's prod path."""

from __future__ import annotations

from datetime import date

import pandas as pd
from pyspark.sql import (
    SparkSession,
    functions as F,
)
from src.custom.qos.isizwe.constants import ITEM_KEY_MAP
from src.custom.qos.zabbix_client import make_api_call, timestamp_to_unix
from src.settings import settings

from dagster import OpExecutionContext


def _custom_replace(value: str) -> str:
    if value in ITEM_KEY_MAP:
        return ITEM_KEY_MAP[value]
    if value.startswith("ubntStaSignal"):
        return "signal"
    return value


def _load_school_lookup(
    spark_session: SparkSession, context: OpExecutionContext
) -> pd.DataFrame:
    try:
        return (
            spark_session.read.table("school_master.zaf")
            .select(
                F.col("school_id_govt").cast("string").alias("school_id_govt"),
                F.col("school_id_giga").cast("string").alias("school_id_giga"),
            )
            .toPandas()
        )
    except Exception as exc:
        context.log.warning(
            f"school_master.zaf lookup unavailable, school_id_giga will be null: {exc}"
        )
        return pd.DataFrame(columns=["school_id_govt", "school_id_giga"])


def fetch_silver_dataframe(
    target_date: date, spark_session: SparkSession, context: OpExecutionContext
) -> pd.DataFrame:
    api_url = settings.ISIZWE_ZABBIX_API_URL
    token = settings.ISIZWE_ZABBIX_TOKEN

    host = make_api_call(api_url, token, "host.get", {"selectTags": ["tag", "value"]})
    host = pd.DataFrame(host)
    host["school_id_govt"] = host["tags"].apply(
        lambda x: x[0]["value"] if x and "value" in x[0] else None
    )
    host = host[~host["school_id_govt"].isna()]
    host = host[host["school_id_govt"] != "N/A"]

    item_params = {
        "output": [
            "itemid",
            "name",
            "key_",
            "hostid",
            "snmp_oid",
            "value_type",
            "status",
        ],
        "hostids": list(host.hostid.unique()),
        "filter": {"key_": list(ITEM_KEY_MAP.keys())},
        "sortfield": "name",
    }
    items = make_api_call(api_url, token, "item.get", item_params)

    items_by_host: dict[str, list] = {}
    for item in items:
        items_by_host.setdefault(item["hostid"], []).append(item)

    start_date = timestamp_to_unix(f"{target_date.isoformat()} 00:00:00")
    end_date = timestamp_to_unix(f"{target_date.isoformat()} 23:59:59")

    all_history_data = []
    for hostid in items_by_host:
        host_items = [i["itemid"] for i in items_by_host[hostid]]
        history_params = {
            "output": "extend",
            "itemids": host_items,
            "time_from": start_date,
            "time_till": end_date,
            "sortfield": "clock",
            "sortorder": "ASC",
        }
        context.log.info(f"getting data for {hostid}")
        history_data = pd.DataFrame(
            make_api_call(api_url, token, "history.get", history_params)
        )
        history_data["hostid"] = hostid
        all_history_data.append(history_data)

    data = (
        pd.concat(all_history_data, ignore_index=True)
        if all_history_data
        else pd.DataFrame()
    )
    if data.empty:
        context.log.warning(f"No Zabbix history data for {target_date}")
        return pd.DataFrame()

    items_df = pd.DataFrame(items).rename(columns={"key_": "key"})
    items_df = items_df[["itemid", "hostid", "key", "name", "value_type"]]
    data = data.merge(items_df, on=["itemid", "hostid"])
    data = data[
        ["hostid", "itemid", "name", "key", "value", "value_type", "clock", "ns"]
    ]

    data["value"] = pd.to_numeric(data["value"])
    data["clock"] = pd.to_numeric(data["clock"])
    data["timestamp"] = pd.to_datetime(data["clock"], unit="s")
    data["value_type"] = data["value_type"].astype(int)
    data = data.rename(columns={"hostid": "host_id"})

    ddf = data
    ddf["key"] = ddf["key"].map(_custom_replace)
    columns = ["timestamp", "host_id", "key"]
    ddf = ddf.sort_values(by=columns, ascending=False)

    wide_df = ddf.pivot_table(
        index=["timestamp", "host_id"],
        columns="key",
        values="value",
        aggfunc="first",
    ).reset_index()

    wide_df["speed_download"] = wide_df["speed_download"] / 1000000
    wide_df["speed_upload"] = wide_df["speed_upload"] / 1000000

    host = host[["hostid", "school_id_govt"]].rename(columns={"hostid": "host_id"})
    wide_df = wide_df.merge(host, on="host_id")
    wide_df = wide_df.sort_values(
        by=["school_id_govt", "host_id", "timestamp"], ascending=False
    )

    wide_df["country_id"] = "ZAF"
    wide_df["provider"] = "Isizwe"

    giga_master = _load_school_lookup(spark_session, context)
    # left join, not the default inner: if school_master.zaf is unavailable,
    # _load_school_lookup returns an empty frame and an inner join would silently
    # drop every row, making the run "succeed" with zero rows uploaded.
    wide_df = wide_df.merge(giga_master, on="school_id_govt", how="left")
    return wide_df


def aggregate_gold_dataframe(silver_df: pd.DataFrame) -> pd.DataFrame:
    df = silver_df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["timestamp"] = df["timestamp"].dt.floor("H")

    ddf = (
        df.groupby(["timestamp", "school_id_giga", "school_id_govt", "host_id"])
        .agg(
            {
                "speed_download": ["min", "mean", "max"],
                "speed_upload": ["min", "mean", "max"],
                "ping_status": [
                    lambda x: x.notna().sum(),
                    lambda x: (x == 1.0).sum(),
                ],
            }
        )
        .reset_index()
    )

    ddf.columns = [
        "timestamp",
        "school_id_giga",
        "school_id_govt",
        "host_id",
        "speed_download_min",
        "speed_download_mean",
        "speed_download_max",
        "speed_upload_min",
        "speed_upload_mean",
        "speed_upload_max",
        "is_connected_all",
        "is_connected_true",
    ]

    ddf["country_id"] = "ZAF"
    ddf["measurement_id"] = "ZAF001"
    ddf["provider"] = "Isizwe"
    return ddf
