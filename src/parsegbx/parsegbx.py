from typing import Union
import bs4
from bs4 import BeautifulSoup

import pandas as pd

# A generic type alias
type T = Union[
    str,
    list[str],
    bs4.element.Tag,
    list[bs4.element.Tag],
    bs4.element.ResultSet[bs4.element.Tag],
    pd.DataFrame,
]


COLUMN_NAMES: T = [
    "Ticket",
    "Open Time",
    "Type",
    "Volume",
    "Asset",
    "Open Price",
    "SL",
    "TP",
    "Close Time",
    "Close Price",
    "Commission",
    "Taxes",
    "Swap",
    "Profit",
]

COLUMNS_TO_REMOVE: T = ["Commission", "Taxes", "Swap"]

COLUMN_TYPES: dict[str, str] = {
    "Type": "str",
    "Volume": "float",
    "Open Price": "float",
    "SL": "float",
    "TP": "float",
    "Close Price": "float",
    "Profit": "float",
}

DATETIME_COLUMN_NAMES: T = ["Open Time", "Close Time"]


def read_htm_from(filename: str, mode: str = "r", encoding: str = "utf-8") -> str:
    """
    Reads the contents of an HTML file and returns it as a string.

    Args:
        filename (str): The name of the HTML file to be read.
        mode (str, optional): The mode in which the file should be opened (default is "r").
        encoding (str, optional): The encoding of the file (default is "utf-8").

    Returns:
        str: The contents of the HTML file as a string.
    """
    with open(filename, mode, encoding=encoding) as file:
        html_content = file.read()
    return html_content


def read_table_from(htm_file: T, parser: str = "html.parser") -> T:
    """Takes an html file and returns a table"""
    soup = BeautifulSoup(htm_file, parser)
    table = soup.find("table")
    return table


def read_rows_from(table: T) -> T:
    """Takes an html file and returns a list of rows"""
    rows: T = []
    if table:
        rows = table.find_all("tr")  # type: ignore
    return rows  # type: ignore


def extract_row_data_from(
    table_rows: T, start_markers: T, continue_markers: T, end_markers: T
) -> T:
    """Extracts the data from the rows"""
    row_data: T = []
    start_processing = False

    for row in table_rows:  # type: ignore
        # First, find Closed Transactions: text and skip it
        if any(marker in row.text for marker in start_markers) and not start_processing:  # type: ignore
            start_processing = True
            continue

        if start_processing:
            if any(marker in row.text for marker in continue_markers):  # type: ignore
                continue

            # End of table
            if any(marker in row.text for marker in end_markers):  # type: ignore
                break

            # Process normally
            # columns = row.find_all(["td", "th"])  # type: ignore
            # data = [col.text for col in columns]  # type: ignore
            row_data.append(row)

    return row_data[:-1]  # type: ignore


def separate_columns_in(rows: T) -> T:
    """Extracts the data from the columns"""
    data_rows: T = []
    try:
        for row in rows:
            columns = row.find_all(["td", "th"])  # type: ignore
            data = [col.text for col in columns]  # type: ignore
            data_rows.append(data)
        return data_rows
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return []


def convert_to_dataframe(data_rows: T) -> T:
    """Converts the data to a dataframe
    Args:
        data_rows (T): The data to be converted to a dataframe
    Returns:
        T: The converted dataframe
    """

    df = pd.DataFrame(data_rows)
    df = df.rename(columns=df.iloc[0]).drop(df.index[0])  # type: ignore

    df.columns = COLUMN_NAMES
    df.drop(columns=COLUMNS_TO_REMOVE, axis=1, inplace=True)  # type: ignore
    df.set_index(df.columns[0], inplace=True, drop=True)  # type: ignore
    return df  # type: ignore


def assign_correct_data_type_to(
    df: T,
    dict_types: dict[str, str] = COLUMN_TYPES,
    datetime_columns: list[str] = DATETIME_COLUMN_NAMES,
) -> T:
    """Assigns the correct datatype to all columns in the dataframe"""
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input `df` must be a pandas DataFrame.")

    df = df.copy()

    for column in datetime_columns:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column])

    for column, col_type in dict_types.items():
        if column in df.columns:
            try:
                if col_type == "numeric":
                    df[column] = pd.to_numeric(df[column], errors="coerce")
                else:
                    df[column] = df[column].astype(  # type: ignore
                        col_type
                    )  # suggestion 4: handle exceptions
            except Exception as e:
                print(
                    f"Error converting column {column} to {col_type}: {e}"
                )  # suggestion 4: handle exceptions
                return df

    return df
