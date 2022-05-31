import numpy as np
import pandas as pd
from datetime import datetime
from collections import abc
from typing import Any, Optional, Union, Dict, List, Tuple


def trading_day_range(bday_start: datetime,
                      bday_end: datetime,
                      bday_freq: Optional[str] = "B",
                      iday_freq: Optional[str] = "T",
                      weekmask=None,
                      tz=None):

    days = pd.bdate_range(start=bday_start, end=bday_end, freq=bday_freq, weekmask=weekmask, tz=tz)

    if iday_freq is not None:
        # Cannot seem to do this in a single call/loop, have to initialise and call union for each following day.
        # Previously used union_many, but this is depreciating.
        index = get_trading_day_range(days[0], freq=iday_freq, tz=tz)

        for d in days[1:]:
            index = index.union(get_trading_day_range(d, freq=iday_freq, tz=tz))
    else:
        index = days

    return index


def get_trading_day_range(day: datetime, freq: str = "T", tz=None):

    open_time = datetime.strptime("09:30", "%H:%M").time()
    close_time = datetime.strptime("16:00", "%H:%M").time()

    open_day = day.replace(hour=open_time.hour, minute=open_time.minute)
    close_day = day.replace(hour=close_time.hour, minute=close_time.minute)

    trade_day = pd.date_range(open_day, close_day, freq=freq, tz=tz)

    # Open guaranteed to be in trade_day, but close is not, so add if needed
    if close_day not in trade_day:
        trade_day = trade_day.insert(len(trade_day), close_day)

    return trade_day


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
