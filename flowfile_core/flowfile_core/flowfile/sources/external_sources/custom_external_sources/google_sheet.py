import gspread
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from typing import Dict, Any, Generator, List
from flowfile_core.schemas.input_schema import GoogleSheet


def getter(data: GoogleSheet) -> Generator[Dict[str, Any], None, None]:
    creds = Credentials(token=data.access_token.get_secret_value())
    gc = gspread.authorize(credentials=creds)
    worksheet = gc.open_by_key(data.sheet_id).worksheet(data.worksheet_name)
    all_values = worksheet.get_values()
    headers = all_values[0]
    for i, value in enumerate(all_values[1:]):
        yield {k: v for k, v in zip(headers, value)}


def initial_getter(data: GoogleSheet):
    def inner_func():
        creds = Credentials(token=data.access_token.get_secret_value())
        service = build('sheets', 'v4', credentials=creds)
        _range = f'{data.worksheet_name}!1:2'
        result = service.spreadsheets().values().get(spreadsheetId=data.sheet_id, range=_range).execute().get('values', [])
        if len(result) > 1:
            align_data(result[0], result[1:])
        return [{k: v for k, v in zip(*result)}]
    return inner_func


def align_data(headers: List[str], values: List[List[str]]) -> None:
    """
    Ensures that the number of columns in 'headers' matches the maximum row length in 'values'.
    If 'headers' has fewer columns, it appends 'unknown_column_{i}' for the missing columns.
    Then aligns all rows in 'values' and 'headers' to have the same length.

    Args:
        headers (List[str]): A list of column names (headers).
        values (List[List[str]]): A list of rows, where each row is a list of strings.

    Returns:
        None: The function modifies 'headers' and 'values' in place.
    """
    # Find the maximum number of values in any row
    max_number_of_values = max(len(row) for row in values)

    # Find the current number of columns
    number_of_cols = len(headers)

    # If headers have fewer columns than the maximum number of values, append missing columns
    if number_of_cols < max_number_of_values:
        headers.extend(f'unknown_column_{i}' for i in range(number_of_cols, max_number_of_values))

    # Align all rows and headers to have the same length
    align_list_len([headers] + values)


def align_list_len(values: List[List[str]], default_value: str = '') -> None:
    """
    Aligns the size of each sublist in a list of lists by appending the default value to shorter sublists.
    Modifies the list of lists in place.

    Args:
        values (List[List[str]]): A list of lists where each sublist may have different lengths.
        default_value (str): The value to append to each sublist to align their lengths (default is an empty string).

    Returns:
        None
    """
    # Determine the maximum length of the sublists
    max_len = max(len(sublist) for sublist in values)

    # Extend each sublist to the maximum length by appending the default value
    for sublist in values:
        sublist.extend([default_value] * (max_len - len(sublist)))
