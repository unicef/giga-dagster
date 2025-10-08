import pandas as pd
import requests

from src.settings import settings


def get_nocodb_table_rows(table_id, offset=0, limit=100, where=None, fields=None):
    """
    Retrieve rows from a specified NoCoDB table and returns them as a list of dictionaries

    Args:
        table_id (str): The ID of the NocoDB table to query.
        offset (int, optional): Number of rows to skip before starting to return rows. Defaults to 0.
        limit (int, optional): Maximum number of rows to return. Defaults to 500.
        where (str, optional): A query string for filtering rows based on conditions.
        fields (str, optional): A string specifying the fields to include in the result.

    Returns:
        list[dict]: A list of dictionaries representing the rows in the table.

    """

    table_url = f"{settings.NOCODB_BASE_URL}/api/v2/tables/{table_id}/records"
    headers = {"xc-token": settings.NOCODB_TOKEN}

    offset = offset or 0
    all_rows = []
    is_last_page = False

    if where and fields:
        params_base = {"where": where, "fields": fields}
    elif where:
        params_base = {"where": where}
    elif fields:
        params_base = {"fields": fields}
    else:
        params_base = {}

    # loop through and get all the data in the table
    while not is_last_page:
        # set the parameters to query the current page of data
        params = {"offset": offset, "limit": limit}
        params = {**params_base, **params}

        # fetch the data from the api
        response = requests.get(table_url, params=params, headers=headers)
        response_json = response.json()
        rows = response_json.get("list", [])
        all_rows.extend(rows)

        # get the page info to determine how to continue
        page_info = response_json.get("pageInfo")
        page_size = page_info.get("pageSize", 0)
        offset += page_size
        is_last_page = page_info.get("isLastPage", False)

    return all_rows


def get_nocodb_table_as_pandas_dataframe(table_id, where=None, fields=None):
    """
    Retrieve data from a NoCoDB table and convert it to a pandas DataFrame.

    Args:
        table_id (str): The ID of the NoCoDB table to query.
        where (str, optional): A query string for filtering rows based on conditions.
        fields (str, optional): A string specifying the fields to include in the result.

    Returns:
        pandas.DataFrame: A DataFrame containing the table data, with empty rows removed.
            Only rows that have at least one non-null value in columns after the third column are included.

    Notes:
        The function removes any rows that have all NULL values in columns after the third column
        (i.e., df.iloc[:, 3:]).
    """

    rows_list = get_nocodb_table_rows(table_id, where=where, fields=fields)
    if not rows_list:
        return pd.DataFrame()

    df = pd.DataFrame(rows_list)
    df = df[df.iloc[:, 3:].notna().any(axis="columns")]
    return df


def get_nocodb_table_as_key_value_mapping(
    table_id, key_column=None, value_column=None, where=None
):
    """
    Convert a NoCoDB table into a mapping of key-value pairs. If columns are not explicitly provided,
    it defaults to using the 4th and 5th columns (index-based) from the fetched data rows.
    Only non-empty key-value pairs are included in the resulting mapping.

    Args:
        table_id (str): The ID of the NoCoDB table to query.
        key_column (str, optional): Name of the column to use as the key. Defaults to None.
        value_column (str, optional): Name of the column to use as the value. Defaults to None.
        where (str, optional): A query string for filtering rows based on conditions.
        fields (str, optional): A string specifying the fields to include in the result.

    Returns:
    dict: key-value mapping derived from the specified table and columns.

    Raises:
    TypeError
        Raised when only one of `key_column` or `value_column` is provided instead of both or neither.
    """
    if any((key_column, value_column)) and not all((key_column, value_column)):
        raise TypeError(
            "Either both key_column and value_column must be provided or neither"
        )
    elif key_column and value_column:
        fields = f"{key_column},{value_column}"
    else:
        fields = None
    rows_list = get_nocodb_table_rows(table_id, where=where, fields=fields)
    if not rows_list:
        return {}

    if not (key_column and value_column):
        columns = list(rows_list[0].keys())
        key_column, value_column = columns[3], columns[4]

    mapping_dict = {
        item[key_column]: item[value_column]
        for item in rows_list
        if (item[key_column] or item[value_column])
    }

    return mapping_dict


def get_nocodb_table_id_from_name(table_name):
    table_name_mappings_id = settings.NOCODB_NAME_MAPPINGS_TABLE_ID
    nocodb_response = get_nocodb_table_rows(
        table_name_mappings_id, where=f"(table_name,eq,{table_name})", fields="table_id"
    )
    if nocodb_response:
        return nocodb_response[0]["table_id"]
    else:
        raise ValueError(f"Unable to retrieve the table_id for table {table_name}")
