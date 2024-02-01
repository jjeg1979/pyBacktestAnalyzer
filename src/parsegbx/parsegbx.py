from functools import reduce, partial
from typing import Union, Callable
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

ParsingPipeline = Callable[[T], T]


def compose(*functions: ParsingPipeline) -> ParsingPipeline:
    """Composes functions into a single function"""
    return reduce(lambda f, g: lambda x: g(f(x)), functions, lambda x: x)  # type: ignore


def read_htm_from(filename: T, mode: T = "r", encoding: T = "utf-8") -> T:
    """
    Reads the contents of an HTML file and returns it as a string.

    Args:
        filename (str): The name of the HTML file to be read.
        mode (str, optional): The mode in which the file should be opened (default is "r").
        encoding (str, optional): The encoding of the file (default is "utf-8").

    Returns:
        str: The contents of the HTML file as a string.
    """
    with open(filename, mode, encoding=encoding) as file:  # type: ignore
        html_content = file.read()  # type: ignore
    return html_content  # type: ignore


def read_table_from(htm_file: T, parser: str = "html.parser") -> T:
    """
    Takes an html file and returns a table.

    Parameters:
    - htm_file (T): The html file to read the table from. It can be a string, a list of strings, a bs4.element.Tag, a list of bs4.element.Tag, a bs4.element.ResultSet[bs4.element.Tag], or a pd.DataFrame.
    - parser (str): The parser to use for parsing the html file. Default is "html.parser".

    Returns:
    - T: The table extracted from the html file. It can be a bs4.element.Tag or a pd.DataFrame.

    """
    soup = BeautifulSoup(htm_file, parser)
    table = soup.find("table")
    return table


def read_rows_from(table: T) -> T:
    """
    Extracts the data from the rows.

    Parameters:
    - table_rows (T): The rows of the table from which to extract data.
    - start_markers (T): The markers indicating the start of data extraction.
    - continue_markers (T): The markers indicating to continue data extraction.
    - end_markers (T): The markers indicating the end of data extraction.

    Returns:
    - T: The extracted row data.

    """
    rows: T = []
    if table:
        rows = table.find_all("tr")  # type: ignore
    return rows  # type: ignore


def extract_row_data_from(
    table_rows: T, start_markers: T, continue_markers: T, end_markers: T
) -> T:
    """
    Extracts the data from the rows.

    Parameters:
    - table_rows (T): The rows of the table from which to extract data.
    - start_markers (T): The markers indicating the start of the data to be extracted.
    - continue_markers (T): The markers indicating that the data extraction should continue.
    - end_markers (T): The markers indicating the end of the data extraction.

    Returns:
    - T: The extracted row data.

    """
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
    """
    Extracts the data from the columns.

    Parameters:
    - rows (T): The input data containing rows of columns.

    Returns:
    - T: The extracted data rows.

    Raises:
    - Exception: If an error occurs during the extraction process.
    """
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
    """
    Assigns the correct datatype to all columns in the dataframe.

    Parameters:
        df (T): The input dataframe. Must be a pandas DataFrame.
        dict_types (dict[str, str], optional): A dictionary mapping column names to their desired data types. Defaults to COLUMN_TYPES.
        datetime_columns (list[str], optional): A list of column names that should be converted to datetime data type. Defaults to DATETIME_COLUMN_NAMES.

    Returns:
        T: The dataframe with the correct data types assigned to its columns.

    Raises:
        ValueError: If the input `df` is not a pandas DataFrame.

    Note:
        - The function makes a copy of the input dataframe to avoid modifying the original dataframe.
        - The function first converts the columns specified in `datetime_columns` to datetime data type using pd.to_datetime().
        - Then, it iterates over the columns specified in `dict_types` and assigns the desired data type to each column using pd.to_numeric() or astype() methods.
        - If any error occurs during the conversion process, the function prints an error message and returns the dataframe without any modifications.

    Example:
        df = pd.DataFrame({'A': ['1', '2', '3'], 'B': ['4', '5', '6']})
        dict_types = {'A': 'numeric', 'B': 'str'}
        datetime_columns = []
        result = assign_correct_data_type_to(df, dict_types, datetime_columns)
        print(result)
        # Output:
        #    A  B
        # 0  1  4
        # 1  2  5
        # 2  3  6
    """
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


parse_gbx_bt: ParsingPipeline = compose(
    partial(read_htm_from, mode="r", encoding="utf-8"),  # type: ignore
    read_table_from,  # type: ignore
    read_rows_from,  # type: ignore
    partial(  # type: ignore
        extract_row_data_from,  # type: ignore
        start_markers=["Closed Transactions:"],  # type: ignore
        continue_markers=["Genbox", "balance", "Deposit"],  # type: ignore
        end_markers=["Closed P/L:"],  # type: ignore
    ),
    separate_columns_in,  # type: ignore
    convert_to_dataframe,  # type: ignore
    assign_correct_data_type_to,  # type: ignore
)
