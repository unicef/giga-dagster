# import pandas as pd

from dagster import asset
from src.functions.get_destination_path_from_filepath import (
    get_destination_path_from_filepath,
)
from src.functions.load_from_adls import load_from_adls
from src.functions.upload_to_adls import save_to_adls

# from dagster import RunRequest, sensor
# # from src.io_managers.common import get_fs_client
# # from src.jobs.data_passthrough import move_to_gold_job
# from src.settings import settings


# @asset
# def list_files(context):
#     path_list = list_filepaths()
#     for path in path_list:
#         if not path.is_directory:
#             # print(path.name + "\n")
#             context.log.info(path.name)

#             destination_filepath = get_destination_path_from_filepath(context, filepath=path.name)

#             context.log.info(destination_filepath)

#             df=load_from_adls(filepath=path.name)

#             if path.name.split('.')[1] == 'csv':
#                 file = df.to_csv(index=False)
#                 save_to_adls(data=file, filename=path.name.split('/')[-1], directory=destination_filepath)

#             # if path.name.split('.')[1] == 'xlsx':
#             #     file = df.to_excel(index=False)

#             # save_to_adls(data=file, filename=path.name.split('/')[-1], directory=destination_filepath)
#     return


@asset
def raw_file(context):
    filepath = "raw/infrastructure-data/Guinea/fiber_nodes_tobevalidated.csv"

    destination_filepath = get_destination_path_from_filepath(
        context, filepath=filepath
    )

    context.log.info(destination_filepath)

    df = load_from_adls(filepath=filepath)

    if filepath.split(".")[1] == "csv":
        file = df.to_csv(index=False)
        filepath_split_list = destination_filepath.split("/")
        destination_path_prefix = "/".join(filepath_split_list[:-1])
        context.log.info(destination_path_prefix)
        context.log.info(filepath.split("/")[-1])
        save_to_adls(
            data=file,
            filename=filepath.split("/")[-1],
            directory=destination_path_prefix,
        )

    # if path.name.split('.')[1] == 'xlsx':
    #     file = df.to_excel(index=False)

    # save_to_adls(data=file, filename=path.name.split('/')[-1], directory=destination_filepath)


# @asset
# def csv_asset():
#     df = pd.DataFrame([1, 2])
#     data = df.to_csv(index=False)
#     return df


# df = pd.DataFrame([1, 2])
# data = df.to_csv(index=False)
# save_to_adls(data, filename="test12df", directory="adls-test")
