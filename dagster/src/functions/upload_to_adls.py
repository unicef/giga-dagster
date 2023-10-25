from io import BytesIO

import pandas as pd
from adls_client import ADLSClient


def save_to_adls(data, filename, directory):
    adls_client = ADLSClient()
    filesystem_client = adls_client._get_file_system_client()

    data_in_bytes = BytesIO(data.encode())
    directory_client = filesystem_client.get_directory_client(directory)
    directory_client.create_file(filename).upload_data(data_in_bytes, overwrite=True)

    return


df = pd.DataFrame([1, 2])
data = df.to_csv(index=False)
save_to_adls(data, filename="test12df", directory="adls-test")
