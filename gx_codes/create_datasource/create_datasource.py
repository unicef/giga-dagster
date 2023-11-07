import os

import great_expectations as gx

context_root_dir = "./great_expectations/"

AZURE_STORAGE_URL = os.environ.get("AZURE_STORAGE_URL")
AZURE_SAS_TOKEN = os.environ.get("AZURE_SAS_TOKEN")

# This will be a function looping through files in the ADLS folder
datasource_name = "azure_blob_storage"
azure_options = {
    "account_url": AZURE_STORAGE_URL,
    "credential": AZURE_SAS_TOKEN,
}

asset_name = "school_geolocation_parquet"
batching_regex = r"(.*).parquet"
abs_container = "giga-dataops-dev"
abs_name_starts_with = "bronze/school-geolocation-data/"


if __name__ == "__main__":
    context = gx.get_context(context_root_dir=context_root_dir)

    datasource = context.get_datasource(datasource_name)
    data_asset = datasource.add_parquet_asset(
        name=asset_name,
        batching_regex=batching_regex,
        abs_container=abs_container,
        abs_name_starts_with=abs_name_starts_with,
    )
