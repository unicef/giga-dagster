import pandas as pd

from dagster import asset


@asset(io_manager_key="adls_csv_io_manager")
def adls_test():
    return pd.DataFrame({"data": range(10)})
