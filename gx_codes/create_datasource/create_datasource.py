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

    # datasource = context.sources.add_pandas_abs(
    #     name=datasource_name, azure_options=azure_options
    # )

# # Build batch request
# batch_request = asset.build_batch_request()
# my_asset = context.get_datasource("bronze_files").get_asset("school_data")

# print(my_asset)
# print(my_asset.batch_request_options)
# my_batch_request = my_asset.build_batch_request()
# my_batch_request = my_asset.build_batch_request(dataframe=dataframe)

# print(my_asset)
# print(my_asset.batch_request_options)
# my_batch_request = my_asset.build_batch_request()
# my_batch_request = my_asset.build_batch_request(dataframe=dataframe)

## Data source config
# context.sources.add_pandas_filesystem(
#     "taxi_multi_batch_datasource",
#     base_directory="./data",  # replace with your data directory
# ).add_csv_asset(
#     "all_years",
#     batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
# )

## BatchRequest config
# all_years_asset: DataAsset = context.datasources[
#     "taxi_multi_batch_datasource"
# ].get_asset("all_years")
# multi_batch_all_years_batch_request: BatchRequest = (
#     all_years_asset.build_batch_request()
# )
