import pandas as pd
from deltalake.writer import write_deltalake

from dagster import asset
from src.settings import settings


@asset
def write_delta_lake():
    df = pd.DataFrame({"data": range(10)})
    write_deltalake(
        f"abfs://{settings.AZURE_BLOB_CONTAINER_NAME}@{settings.AZURE_BLOB_SAS_HOST}/out/deltars_table",
        df,
        mode="overwrite",
        configuration={
            "enableChangeDataFeed": "true",
        },
        storage_options={
            "AZURE_STORAGE_SAS_TOKEN": settings.AZURE_SAS_TOKEN,
        },
    )
