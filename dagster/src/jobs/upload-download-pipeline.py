from dagster import asset


@asset
def upload_pipeline() -> None:
    return


@asset
def download_pipeline() -> None:
    return
