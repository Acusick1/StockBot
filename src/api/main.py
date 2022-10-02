import requests
import json
import pandas as pd
import yfinance as yf
from datetime import timedelta
from typing import Union, Optional, Dict
from src.db import schemas
from config import yahoo_api_settings


class FinanceApi:

    def __init__(self,
                 group_by: str = "ticker",
                 auto_adjust: bool = False,
                 prepost: bool = False,
                 threads: Union[bool, int] = True,
                 proxy: Optional[bool] = None,
                 **kwargs):
        """Download financial data using yfinance API
            :param group_by: group by ticker (to access via data['SPY'])
                (optional, default is 'ticker')
            :param auto_adjust: adjust all OHLC automatically
            :param prepost: download pre-/post-regular market hours data
            :param threads: use threads for mass downloading? (True/False/Integer)
            :param proxy: use proxy server to download data
            :param kwargs: additional arguments that can be passed to the API (see yfinance documentation)
        """

        params = {
            "group_by": group_by,
            "auto_adjust": auto_adjust,
            "prepost": prepost,
            "threads": threads,
            "proxy": proxy,
            **kwargs
        }

        self.default_params = params

    def make_request(
            self,
            request: schemas.RequestBase,
            interval_key: Optional[str] = None,
            **kwargs) -> pd.DataFrame:

        """Make request to API for input stocks, using default parameters set during initialisation, and merge with
        existing database data.
        :param request: request schema with all information needed to make an API request
        :param interval_key: optional interval key, if none provided request interval will be used (default)
        :param kwargs: arguments to be passed directly to API, allows additional arguments to be specified or defaults
            overwritten.
        """

        # Take copy of default arguments, and update with key word arguments if passed
        params = self.default_params.copy()
        params.update(kwargs)

        tickers = request.stock if isinstance(request.stock, str) else ",".join(request.stock)
        start_date = request.start_date
        end_date = request.end_date
        period = request.period

        if interval_key is None:
            interval_key = request.interval.key
        
        # Adding day to requested dates as yfinance seems to be a day off for day requests
        if "d" in interval_key:
            start_date += timedelta(days=1)
            end_date += timedelta(days=1)

        output = yf.download(
            tickers=tickers,
            start=start_date,
            end=end_date,
            period=period,
            interval=interval_key,
            **params
        )

        if output is not None:

            if "m" in interval_key and output.shape[0] > 2:
                # Ensure data is consistent. Downloading date range data will automatically append most recent days
                # closing data (looks to be a bug in yfinance)
                # TODO: Just compare tail delta to mapped interval value here
                head_delta = int((output.index[1] - output.index[0]).total_seconds())
                tail_delta = int((output.index[-1] - output.index[-2]).total_seconds())
                if tail_delta != head_delta:
                    output.drop(output.index[-1], inplace=True)

        return output


class YahooApi:

    api_key = yahoo_api_settings.api_key

    def __init__(self):

        self.headers = {'x-api-key': self.api_key}

    def get_stock_history(self, request: schemas.RequestBase):

        endpoint = "https://yfapi.net/v8/finance/spark"

        symbols = request.stock if isinstance(request.stock, str) else ",".join(request.stock)

        params = {"symbols": symbols,
                  "period": request.period,
                  "interval": request.interval}

        return self.make_request(endpoint, params=params)

    def make_request(self, endpoint: str, params: Dict):

        return requests.request("GET", endpoint, headers=self.headers, params=params)

    @staticmethod
    def parse_response(response):

        return json.loads(response.content.decode('utf-8'))


if __name__ == "__main__":
    pass