from typing import Union
import bs4
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
