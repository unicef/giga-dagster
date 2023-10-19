import pandas as pd
from deltalake import DeltaTable, write_deltalake

from dagster import ConfigurableIOManager, InputContext, OutputContext
from src.settings import settings

from .common import get_file_path_from_context


class Adls2DeltaLakeIOManager(ConfigurableIOManager):
    path_prefix = "fake-gold"

    def _get_delta_config(self, filename: str):
        return dict(
            table_or_uri=f"abfs://{settings.AZURE_BLOB_CONTAINER_NAME}@{settings.AZURE_BLOB_SAS_HOST}/{self.path_prefix}/{filename}",
            storage_options={
                "AZURE_STORAGE_SAS_TOKEN": settings.AZURE_SAS_TOKEN,
            },
        )

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        filename = get_file_path_from_context(context)
        write_deltalake(
            **self._get_delta_config(filename),
            data=obj,
            mode="overwrite",
            configuration={
                # TODO: This does not actually enable CDF
                "enableChangeDataFeed": "true",
            },
        )

    def load_input(self, context: InputContext) -> pd.DataFrame:
        dt = DeltaTable(**self._get_delta_config(get_file_path_from_context(context)))
        return dt.to_pandas()
