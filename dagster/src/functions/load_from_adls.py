from io import BytesIO

import pandas as pd
from adls_client import ADLSClient


def load_from_adls(filepath):
    adls_client = ADLSClient()
    filesystem_client = adls_client._get_file_system_client()
    file_client = filesystem_client.get_file_client(filepath)
    file = file_client.download_file().readall()

    return pd.read_csv(BytesIO(file))


filepath = "raw/infrastructure-data/Guinea/fiber_nodes_tobevalidated.csv"
load_from_adls(filepath)
