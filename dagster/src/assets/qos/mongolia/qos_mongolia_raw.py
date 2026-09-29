import datetime as dt

from src.custom.qos.mongolia.common import fetch_device_snapshots
from src.utils.adls import ADLSFileClient

from dagster import OpExecutionContext, Output, asset


@asset
def mongolia_qos_raw_json(
    context: OpExecutionContext, adls_file_client: ADLSFileClient
) -> Output:
    """Polls every whitelisted Mongolia device and lands its raw JSON response on
    ADLS. Ports get_bandwidth_utilization.py - runs every 5 minutes, same as prod."""
    run_timestamp = dt.datetime.now()
    fetch_device_snapshots(adls_file_client, run_timestamp, context)
    return Output(None, metadata={"run_timestamp": run_timestamp.isoformat()})
