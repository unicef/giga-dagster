import pandas as pd
import requests

from src.settings import settings


def get_nocodb_table_rows(view_id, table_id, offset=0, limit=100, where=""):
    """
    Retrieve rows from a specified NoCoDB table and returns them as a list of dictionaries

    Args:
        view_id (str): The ID of the NocoDB view to query.
        table_id (str): The ID of the NocoDB table to query.
        offset (int, optional): Number of rows to skip before starting to return rows. Defaults to 0.
        limit (int, optional): Maximum number of rows to return. Defaults to 500.
        where (str, optional): SQL-like WHERE clause to filter the results. Defaults to empty string.

    Returns:
        list[dict]: A list of dictionaries representing the rows in the table.

    """

    table_url = f"{settings.NOCODB_BASE_URL}/api/v2/tables/{table_id}/records"
    headers = {"xc-token": settings.NOCODB_TOKEN}

    offset = offset or 0
    all_rows = []
    is_last_page = False

    # loop through and get all the data in the table
    while not is_last_page:
        # set the parameters to query the current page of data
        params = {"offset": offset, "limit": limit, "where": where, "viewID": view_id}

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

    return


def get_nocodb_table_as_pandas_dataframe(
    view_id, table_id, offset=0, limit=500, where=""
):
    """
    Retrieve data from a NoCoDB table and convert it to a pandas DataFrame.

    Args:
        view_id (str): The ID of the NoCoDB view to query.
        table_id (str): The ID of the NoCoDB table to query.
        offset (int, optional): Number of rows to skip before starting to return rows. Defaults to 0.
        limit (int, optional): Maximum number of rows to return. Defaults to 500.
        where (str, optional): SQL-like WHERE clause to filter the results. Defaults to empty string.

    Returns:
        pandas.DataFrame: A DataFrame containing the table data, with empty rows removed.
            Only rows that have at least one non-null value in columns after the third column are included.

    Notes:
        The function removes any rows that have all NULL values in columns after the third column
        (i.e., df.iloc[:, 3:]).
    """

    rows_list = get_nocodb_table_rows(
        view_id, table_id, offset=offset, limit=limit, where=where
    )
    df = pd.DataFrame(rows_list)
    df = df[df.iloc[:, 3:].notna().any(axis="columns")]
    return df


def get_nocodb_table_as_key_value_mapping(
    view_id, table_id, key_column=None, value_column=None, offset=0, limit=500, where=""
):
    """
    Convert a NoCoDB table into a mapping of key-value pairs. If columns are not explicitly provided,
    it defaults to using the 4th and 5th columns (index-based) from the fetched data rows.
    Only non-empty key-value pairs are included in the resulting mapping.

    Args:
        view_id (str): The ID of the NoCoDB view to query.
        table_id (str): The ID of the NoCoDB table to query.
        key_column (str, optional): Name of the column to use as the key. Defaults to None.
        value_column (str, optional): Name of the column to use as the value. Defaults to None.
        offset (int, optional): Number of rows to skip before starting to return rows. Defaults to 0.
        limit (int, optional): Maximum number of rows to return. Defaults to 500.
        where (str, optional): SQL-like WHERE clause to filter the results. Defaults to empty string.

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
    rows_list = get_nocodb_table_rows(
        view_id, table_id, offset=offset, limit=limit, where=where
    )

    if not (key_column and value_column):
        columns = list(rows_list[0].keys())
        key_column, value_column = columns[3], columns[4]

    mapping_dict = {
        item[key_column]: item[value_column]
        for item in rows_list
        if (item[key_column] or item[value_column])
    }

    return mapping_dict
