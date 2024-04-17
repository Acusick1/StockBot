from zoneinfo import ZoneInfo

import pandas as pd
import pandera as pa
from pandera.typing import DataFrame, Series
from pydantic import BaseModel

nytz = pd.DatetimeTZDtype(tz=ZoneInfo("America/New_York"))


class StockBase(BaseModel):
    ticker: str


class Daily(pa.DataFrameModel):
    stock_id: Series[str]
    timestamp: Series[pd.DatetimeTZDtype] = pa.Field(dtype_kwargs={"tz": ZoneInfo("America/New_York")}, coerce=True)
    open: Series[float]
    high: Series[float]
    low: Series[float]
    close: Series[float]
    adj_close: Series[float]
    volume: Series[int]


class Backtest(pa.DataFrameModel):
    stock_id: Series[str]
    Open: Series[float]
    High: Series[float]
    Low: Series[float]
    Close: Series[float]
    Volume: Series[int]


@pa.check_types
def daily_to_backtest(df: DataFrame[Daily]) -> DataFrame[Backtest]:
    out_df = df.copy()
    out_df = out_df.set_index(Daily.timestamp)
    out_df = out_df.drop(columns=[Daily.close])
    return out_df.rename(
        columns={
            Daily.stock_id: Backtest.stock_id,
            Daily.open: Backtest.Open,
            Daily.high: Backtest.High,
            Daily.low: Backtest.Low,
            Daily.adj_close: Backtest.Close,
            Daily.volume: Backtest.Volume,
        }
    )
