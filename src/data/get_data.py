import time
import yfinance as yf
from typing import Union, Optional

valid_periods = ("1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max")
valid_intervals = ("1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo")


def get_example_data():

    return download_data(tickers=["AAPL", "F"], period="1y")


def download_data(tickers: Union[list, tuple],
                  period: valid_periods = "1mo",
                  interval: valid_intervals = "1d",
                  start: str = None,
                  end: str = None,
                  group_by: str = "ticker",
                  auto_adjust: bool = False,
                  prepost: bool = False,
                  threads: Union[bool, int] = True,
                  proxy: Optional[bool] = None,
                  **kwargs):
    """Download financial data using yfinance API
        :param tickers: list or string as well
        :param period: use "period" instead of start/end
        :param interval: fetch data by interval (including intraday if period < 60 days)
        :param start: string with start date, used with end instead of period/interval
        :param end: string with end date
        :param group_by: group by ticker (to access via data['SPY'])
            (optional, default is 'ticker')
        :param auto_adjust: adjust all OHLC automatically
        :param prepost: download pre/post regular market hours data
        :param threads: use threads for mass downloading? (True/False/Integer)
        :param proxy: use proxy server to download data
    """

    # Gather all function arguments to dict
    args = locals().copy()

    # If additional key word arguments passed, replace kwarg nested item with items per argument
    if args.get("kwargs") is not None:
        args.pop("kwargs")
        args.update(kwargs)

    # Time period must be either period/interval (default) or start/end, remove not needed keys
    drop_keys = ("start", "end") if args.get("start") is None else ("period", "interval")

    for key in drop_keys:
        args.pop(key)

    args.pop("tickers")
    data = {}
    for ticker in tickers:
        data[ticker] = yf.download(ticker, **args)

        if ticker != tickers[-1]:
            time.sleep(1)

    return data
