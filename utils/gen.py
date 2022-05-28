import numpy as np
import pandas as pd
from datetime import datetime
from collections import abc
from typing import Any, Union, Dict, List, Tuple


def validate_strict_args(inp: Any, options: Union[List, Tuple], name: str, optional: bool = False) -> None:

    if not (inp in options or (optional and inp is None)):
        raise ValueError(f"Inputs {name}: {inp} must be one of: {options}")


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


def get_key_from_value(d: Dict[str, Any], v: Any):

    return list(d.keys())[list(d.values()).index(v)]


def validate_date_format(inp: str, form: str = '%Y-%m-%d'):

    try:
        datetime.strptime(inp, form)
    except ValueError:
        raise ValueError(f"Incorrect date format input: {inp}. Format required is: {form}")
