from dagster import Config


class FileConfig(Config):
    filepath: str
    dataset_type: str
    metadata: dict
    file_size_bytes: int
    metastore_schema: str
