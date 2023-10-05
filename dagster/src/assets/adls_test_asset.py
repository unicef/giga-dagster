import pandas as pd

from dagster import asset


@asset
def dataframe_asset():
    return pd.DataFrame({"data": range(5)})
