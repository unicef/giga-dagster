from adls_client import ADLSClient


def list_filepaths(path_prefix):
    adls_client = ADLSClient()
    filesystem_client = adls_client._get_file_system_client()
    path_list = filesystem_client.get_paths(path_prefix)

    return path_list


def list_files(path_prefix):
    path_list = list_filepaths(path_prefix)
    for path in path_list:
        if not path.is_directory:
            print(path.name + "\n")


list_files(path_prefix="raw")
