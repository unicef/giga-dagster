import pandas as pd
from dagster import op, job

from src.utils.adls import load_csv, save_delta


@op
def get_raw_data() -> pd.DataFrame:
    return load_csv("adls-test", "adls_test.csv")


@op
def convert_csv_to_delta(get_raw_data: pd.DataFrame):
    save_delta(get_raw_data, "fake-gold", "adls-test")


@job
def raw_to_delta():
    convert_csv_to_delta(get_raw_data())
