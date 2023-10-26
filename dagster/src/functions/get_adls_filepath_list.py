from src.functions.adls_client import ADLSClient

# from dagster import asset

# from dagster import Config

# class PathConfig(Config):
#     path_prefix: str


def list_filepaths(path_prefix="raw"):
    adls_client = ADLSClient()
    filesystem_client = adls_client._get_file_system_client()
    path_list = filesystem_client.get_paths(path_prefix)
    return path_list


# @asset
# def list_files(context, path_prefix):
#     path_list = list_filepaths(path_prefix)
#     for path in path_list:
#         if not path.is_directory:
#             # print(path.name + "\n")
#             context.log.info(path.name)


# list_files(path_prefix="raw")
