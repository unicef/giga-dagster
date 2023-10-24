import pandas as pd
import pandas.api.types as pd_types

from dagster import Config, job, op
from src.utils.adls import adls_loader, adls_saver


class FileConfig(Config):
    source_path_prefix: str
    destination_path_prefix: str
    filename: str


@op
def get_file(config: FileConfig):
    return adls_loader(config.source_path_prefix, config.filename)


@op
def move_to_gold_as_delta_table(config: FileConfig):
    delta_filename = ".".join(config.filename.split(".")[:-1])
    df: pd.DataFrame = get_file(config)
    for column in df.columns:
        if pd_types.is_integer_dtype(df[column].dtype) or pd_types.is_float_dtype(
            df[column].dtype
        ):
            continue

        try:
            df[column] = df[column].astype(int)
        except ValueError:
            try:
                df[column] = df[column].astype(float)
            except ValueError:
                df[column] = df[column].astype(str)
    adls_saver(df, config.destination_path_prefix, delta_filename, "delta")


@job
def move_to_gold_job():
    move_to_gold_as_delta_table()
