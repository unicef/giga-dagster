from dagster import define_asset_job
from src.assets.upload_processing.parquet_to_delta import convert_parquets_to_delta

upload_processing_job = define_asset_job(
    name="upload_processing_job", selection=[convert_parquets_to_delta]
)
