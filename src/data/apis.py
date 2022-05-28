import os
import requests
import json
import h5py
import time
import pandas as pd
import yfinance as yf
from typing import Union, Optional, Dict, List, Tuple, Any
from datetime import datetime
from src.data.database import merge_data
from utils.gen import dataframe_from_dict, validate_date_format, validate_strict_args
from utils.hdf5 import create_h5_key
from src.settings import VALID_PERIODS, VALID_INTERVALS, TIME_IN_SECONDS, STOCK_HISTORY_FILE


class FinanceApi:

    def __init__(self,
                 interval: str = "1d",
                 period: Optional[str] = None,
                 start: Optional[str] = None,
                 end: Optional[str] = None,
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
            :param kwargs: additional arguments that can be passed to the API (see yfinance documentation)
        """

        validate_strict_args(interval, options=VALID_INTERVALS, name="interval", optional=True)
        validate_strict_args(period, options=VALID_PERIODS, name="period", optional=True)

        # Gather all function arguments to dict
        args = locals().copy()
        args.update(args.pop("kwargs"))
        del args["self"]

        self.args = args

    @staticmethod
    def period_or_range(args: Dict[str, Any]):

        # Time period must be either period/interval (default) or start/end, remove not needed keys
        start = args.get("start")
        end = args.get("end")
        period = args.get("period")

        if period is None:
            if start is None:
                raise KeyError("A period or start/end date must be provided ")

            else:
                for date in (start, end):
                    validate_date_format(date, "%Y-%m-%d")

                args.pop("period")
        else:
            for key in ("start", "end"):
                args.pop(key)

        return args

    def make_request(self, tickers: Union[list, tuple], save: bool = True, **kwargs):
        """Make request to API for input stocks, using default parameters set during initialisation, and merge with
        existing database data
        :param tickers: Stocks to be downloaded.
        :param save: Option to save downloaded data to database.
        :param kwargs: arguments to be passed directly to API, allows additional arguments to be specified or defaults
            overwritten.
        """
        # Take copy of default arguments, and update with key word arguments if passed
        args = self.args.copy()

        if kwargs:
            args.update(kwargs)

        args = self.period_or_range(args)

        data = {}
        for ticker in tickers:
            output = yf.download(ticker, **args)

            if output is not None:
                # Ensure data is consistent. Downloading date range data will automatically append most recent days
                # closing data (looks to be a bug in yfinance)
                tail_delta = int((output.index[-1] - output.index[-2]).total_seconds())
                if tail_delta != TIME_IN_SECONDS[args["interval"]]:
                    output.drop(output.index[-1], inplace=True)

                if save:
                    with pd.HDFStore(STOCK_HISTORY_FILE) as h5:
                        merge_data(output, h5_file=h5, key=create_h5_key(args['interval'], ticker))

            data[ticker] = output

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
                          period: str = "1mo",
                          interval: str = "1d"):

        validate_strict_args(interval, options=VALID_INTERVALS, name="interval")
        validate_strict_args(period, options=VALID_PERIODS, name="period")

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
