import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
from datetime import datetime, time
from collections import abc
from typing import Any, Optional, Union, Dict, List, Tuple


def trading_day_range(bday_start: datetime,
                      bday_end: datetime,
                      bday_freq: Optional[str] = "B",
                      iday_freq: Optional[str] = "T",
                      market: str = "NYSE"):

    # Get market calendar
    market_calendar = mcal.get_calendar(market)
    market_days = market_calendar.schedule(start_date=bday_start, end_date=bday_end)

    # Get filtered days if frequency is higher than daily
    filtered_index = pd.bdate_range(start=bday_start, end=bday_end, freq=bday_freq)

    # Get intersection between the two
    days = market_days.index.intersection(filtered_index)

    if days.shape[0] and iday_freq is not None:
        # Cannot seem to do this in a single call/loop, have to initialise and call union for each following day.
        # Previously used union_many, but this is depreciating.
        index = get_trading_day_range(
            days[0],
            market_open=market_calendar.open_time,
            market_close=market_calendar.close_time,
            freq=iday_freq)

        for d in days[1:]:
            index = index.union(
                get_trading_day_range(
                    d,
                    market_open=market_calendar.open_time,
                    market_close=market_calendar.close_time,
                    freq=iday_freq))
    else:
        index = days

    return index


def get_trading_day_range(day: datetime, market_open=time(9, 30), market_close=time(16), freq: str = "T"):

    open_day = day.replace(hour=market_open.hour, minute=market_open.minute, tzinfo=market_open.tzinfo)
    close_day = day.replace(hour=market_close.hour, minute=market_close.minute, tzinfo=market_open.tzinfo)

    trade_day = pd.date_range(open_day, close_day, freq=freq, tz=market_open.tzinfo)

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
