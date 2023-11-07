# import pandas as pd

# from dagster import op

# from deltalake.writer import write_deltalake
# from src.settings import AZURE_BLOB_CONTAINER_NAME, AZURE_BLOB_SAS_HOST, AZURE_SAS_TOKEN


# @op
# def write_delta_lake_poc():
#     df = pd.DataFrame({"data": range(10)})
#     write_deltalake(
#         f"abfs://{AZURE_BLOB_CONTAINER_NAME}@{AZURE_BLOB_SAS_HOST}/out/deltars_table",
#         df,
#         mode="overwrite",
#         configuration={
#             "enableChangeDataFeed": "true",
#         },
#         storage_options={
#             "AZURE_STORAGE_SAS_TOKEN": AZURE_SAS_TOKEN,
#         },
#     )
