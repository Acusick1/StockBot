import os
import requests
import json
import h5py
import time
import pandas as pd
import yfinance as yf
from typing import Union, Optional, Dict, List, Tuple
from datetime import datetime
from src.gen import dataframe_from_dict

valid_periods = ("1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max")
valid_intervals = ("1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo")


def get_example_data():
    api = FinanceApi(period="1y")
    return api.download_data(tickers=["AAPL", "F"])


def check_database():
    pass


class FinanceApi:

    def __init__(self,
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

        self.args = args

    def download_data(self, tickers: Union[list, tuple]):

        data = {}
        for ticker in tickers:
            data[ticker] = yf.download(ticker, **self.args)

            if ticker != tickers[-1]:
                time.sleep(1)

        return data


class YahooApi:

    def __init__(self):

        api_key = os.environ.get('YAHOO_FINANCE_API_KEY')

        if api_key is None:
            raise EnvironmentError(
                "YAHOO_FINANCE_API_KEY environment variable not found, ensure it is added and refresh session"
            )

        self.headers = {'x-api-key': api_key}

    def get_stock_history(self,
                          symbols: Union[List, Tuple],
                          period: valid_periods = "1mo",
                          interval: valid_intervals = "1d"):

        endpoint = "https://yfapi.net/v8/finance/spark"

        params = {"symbols": ",".join(symbols),
                  "period": period,
                  "interval": interval}

        return self.parse_response(self.make_request(endpoint, params=params))

    def make_request(self, endpoint: str, params: Dict):

        return requests.request("GET", endpoint, headers=self.headers, params=params)

    @staticmethod
    def parse_response(response):

        return json.loads(response.content.decode('utf-8'))

    def get_historical_data(self, stocks):
        now = datetime.now()
        filename = "data_" + now.strftime("%d_%b_%Y") + '.hdf5'

        with h5py.File(filename, 'a') as f:
            saved_keys = list(f.keys())
            print(saved_keys)

        dfs = {s: pd.read_hdf(filename, key=s) for s in stocks if s in saved_keys}
        stocks = [s for s in stocks if s not in dfs.keys()]

        if stocks:
            content = self.get_stock_history(stocks, period="5y")

            for stock, data in content.items():
                data['timestamp'] = pd.to_datetime(data['timestamp'], unit='s')
                df = dataframe_from_dict(data)
                df.set_index('timestamp', inplace=True)
                dfs[stock] = df

                df.to_hdf(filename, key=stock, mode='a')

        return dfs


if __name__ == "__main__":
    pass
