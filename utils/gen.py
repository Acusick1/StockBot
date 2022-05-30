import numpy as np
import pandas as pd
from datetime import datetime
from collections import abc
from typing import Any, Union, Dict, List, Tuple


def trading_day_range(bday_start=datetime.date,
                      bday_end=datetime.date,
                      bday_freq="B",
                      iday_freq="T",
                      weekmask=None,
                      tz=None):
    # TODO: Refactor and clean

    open_time = datetime.strptime("09:30", "%H:%M").time()
    close_time = datetime.strptime("16:00", "%H:%M").time()

    for i, d in enumerate(pd.bdate_range(start=bday_start, end=bday_end, freq=bday_freq, weekmask=weekmask)):

        d1 = d.replace(hour=open_time.hour, minute=open_time.minute)
        d2 = d.replace(hour=close_time.hour, minute=close_time.minute)

        day = pd.date_range(d1, d2, freq=iday_freq, tz=tz)

        if i == 0:
            index = day
        else:
            index = index.union(day)

    return index


def validate_strict_args(inp: Any, options: Union[List, Tuple], name: str, optional: bool = False) -> None:
    if not (inp in options or (optional and inp is None)):
        raise ValueError(f"Inputs {name}: {inp} must be one of: {options}")


def pct_change(data: np.array):
    if len(data) == 1:
        out = 0
    else:
        out = (data[1:] - data[:-1]) / data[:-1]

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
