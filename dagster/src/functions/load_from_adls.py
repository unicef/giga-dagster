from io import BytesIO

import pandas as pd

from src.functions.adls_client import ADLSClient


def load_from_adls(filepath):
    adls_client = ADLSClient()
    filesystem_client = adls_client._get_file_system_client()
    file_client = filesystem_client.get_file_client(filepath)
    buffer = BytesIO()
    file_client.download_file().readinto(buffer)
    buffer.seek(0)

    if filepath.split(".")[1] == "csv":
        data = pd.read_csv(buffer)

    # if filepath.split('.')[1] == 'xlsx':
    #     data = pd.read_excel(buffer)

    return data


# filepath = "raw/infrastructure-data/Guinea/fiber_nodes_tobevalidated.csv"
# load_from_adls(filepath)
