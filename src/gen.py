import h5py
import numpy as np
import pandas as pd
from datetime import datetime
from collections import abc
from typing import Any, Union, Dict, List


def pct_change(data: np.array):

    if len(data) == 1:
        out = 0
    else:
        out = (data[1:] - data[:-1])/data[:-1]

    return out


def dataframe_from_dict(d: dict) -> pd.DataFrame:
    """Creating pandas dataframe from dictionary.
    Wrapper for pd.DataFrame.from_dict for dictionaries with metadata, which will propagate singleton key value pairs
    across full dataframe. Uses dataframe attrs, which is for metadata, but still experimental"""

    df = pd.DataFrame.from_dict(d)

    for key, value in d.items():
        if not isinstance(value, abc.Iterable) or isinstance(value, str):
            df.drop(columns=key, inplace=True)
            df.attrs[key] = value

    return df


def get_subplot_shape(num: int, max_columns: int = 8) -> (int, int):

    columns = min(round(np.sqrt(num)), max_columns)
    quotient, rem = divmod(num, columns)
    rows = quotient + 1 if rem else quotient
    return rows, columns


def keys_in_h5(file_path) -> list:
    """Return keys in h5 file, only necessary if not using pandas (otherwise use HDFStore.keys())"""
    with h5py.File(file_path, 'r') as f:
        saved_keys = list(f.keys())

    return saved_keys


def create_h5_key(*args: str) -> str:
    """Create a valid multi-level h5 key from input strings"""
    return "/".join(["", *args])


def h5_key_elements(key: str, index: int = None) -> Union[List, str]:
    """Return h5 key levels from input key
    :param key: HDF5 key, input as string
    :param index: index or level of nested key to return
    """

    elements = key.lstrip("/").split("/")

    if index is None:
        return elements
    else:
        return elements[index]


def get_key_from_value(d: Dict[str, Any], v: Any):

    return list(d.keys())[list(d.values()).index(v)]


def validate_date_format(inp: str, form: str = '%Y-%m-%d'):

    try:
        datetime.strptime(inp, form)
    except ValueError:
        raise ValueError(f"Incorrect date format input: {inp}. Format required is: {form}")
