from src.sensors.base import FileConfig


def get_output_metadata(config: FileConfig, filepath: str = None):
    metadata = {
        **config.dict(exclude={"metadata"}),
        **config.metadata,
    }
    if filepath is not None:
        metadata["filepath"] = filepath

    return metadata
