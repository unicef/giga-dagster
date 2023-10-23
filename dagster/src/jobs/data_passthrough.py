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
    df = get_file(config).fillna("")
    adls_saver(df, config.destination_path_prefix, delta_filename, "delta")


@job
def move_to_gold_job():
    move_to_gold_as_delta_table()
