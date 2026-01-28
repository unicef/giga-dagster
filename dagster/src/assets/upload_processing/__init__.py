from .parquet_to_delta import convert_parquets_to_delta

GROUP_NAME = "upload_processing"

assets = [convert_parquets_to_delta]
