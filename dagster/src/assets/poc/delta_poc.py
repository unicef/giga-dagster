import pandas as pd

from dagster import asset


@asset(io_manager_key="adls_delta_io_manager")
def delta_poc():
    return pd.DataFrame({"data": range(20)})
