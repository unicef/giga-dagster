from src.sensors.base import AssetFileConfig


def get_output_metadata(config: AssetFileConfig, filepath: str = None):
    metadata = {
        **config.dict(exclude={"metadata"}),
        **config.metadata,
    }
    if filepath is not None:
        metadata["filepath"] = filepath

    return metadata
