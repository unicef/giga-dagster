import pandas as pd
from deltalake.writer import write_deltalake

from dagster import asset


@asset
def write_delta_lake_asset():
    df = pd.DataFrame({"data": range(10)})
    write_deltalake(
        "azure_url",
        df,
        mode="append",
    )
