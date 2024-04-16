import json
from datetime import datetime, timezone
from time import sleep

import pandas as pd
import requests
import yfinance as yf
from pandera import check_output
from pandera.typing import DataFrame

from config import yahoo_api_settings
from src.db import schemas
from src.time_db.schemas import Daily, nytz
from utils.gen import batch


class FinanceApi:
    last_request = None
    poll_frequency = yahoo_api_settings.poll_frequency
    max_stocks_per_request = yahoo_api_settings.max_stocks_per_request

    def __init__(
        self,
        group_by: str = "ticker",
        auto_adjust: bool = False,
        prepost: bool = False,
        threads: bool | int = True,
        proxy: bool | None = None,
        **kwargs,
    ):
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
            **kwargs,
        }

        self.default_params = params

    def request(self, stock: list[str], **kwargs):
        req = schemas.RequestBase(stock=stock, **kwargs)
        return self.make_request(req)

    @check_output(schema=Daily.to_schema())
    def make_request(
        self, request: schemas.RequestBase, interval_key: str | None = None, market: str = "NYSE", **kwargs
    ) -> DataFrame[Daily]:
        """Make request to API for input stocks, using default parameters set during initialisation, and merge with
        existing database data.
        :param request: request schema with all information needed to make an API request
        :param interval_key: optional interval key, if none provided request interval will be used (default)
        :param kwargs: arguments to be passed directly to API, allows additional arguments to be specified or defaults
            overwritten.
        """

        tickers = [request.stock] if isinstance(request.stock, str) else request.stock
        period = request.period

        if interval_key is None:
            interval_key = request.interval.key

        output = self._download(tickers=tickers, period=period, interval=interval_key, **kwargs)

        if output is None:
            return None

        # Making a multi-index for consistent response
        if len(tickers) == 1:
            output = pd.concat({tickers[0]: output}, axis=1)

        if "m" in interval_key and output.shape[0] > 2:
            # Ensure data is consistent. Downloading date range data will automatically append most recent days
            # closing data (looks to be a bug in yfinance)
            # TODO: Just compare tail delta to mapped interval value here
            head_delta = int((output.index[1] - output.index[0]).total_seconds())
            tail_delta = int((output.index[-1] - output.index[-2]).total_seconds())
            if tail_delta != head_delta:
                output = output.drop(output.index[-1])

        # Converting multiindex to dataframe by stacking stock name index to column
        output.index = output.index.tz_localize(nytz.tz)
        output = output.stack(level=0, future_stack=True).reset_index()  # noqa: PD013
        output.columns = [x.lower().replace(" ", "_") for x in output.columns]
        return output.rename(columns={"date": Daily.timestamp, "level_0": Daily.timestamp, "level_1": Daily.stock_id})

    @batch(size=yahoo_api_settings.max_stocks_per_request, concat_axis=1)
    def _download(self, tickers, **kwargs):
        """
        Wrapper around 'yfinance.download' to request data, while respecting the API poll frequency

        Parameters
        ----------
        kwargs: additional parameters to pass to the yfinance API
        """

        # Take copy of default arguments, and update with key word arguments if passed
        params = self.default_params.copy()
        params.update(kwargs)

        since_last = (
            (datetime.now(tz=timezone.utc) - self.last_request).total_seconds()
            if self.last_request is not None
            else float("inf")
        )
        if self.poll_frequency > since_last:
            sleep(self.poll_frequency - since_last)

        output = yf.download(tickers, **params)
        self.last_request = datetime.now(tz=timezone.utc)

        return output


class YahooApi:
    api_key = yahoo_api_settings.api_key

    def __init__(self):
        self.headers = {"x-api-key": self.api_key}

    def get_stock_history(self, request: schemas.RequestBase):
        endpoint = "https://yfapi.net/v8/finance/spark"

        symbols = request.stock if isinstance(request.stock, str) else ",".join(request.stock)

        params = {
            "symbols": symbols,
            "period": request.period,
            "interval": request.interval,
        }

        return self.make_request(endpoint, params=params)

    def make_request(self, endpoint: str, params: dict):
        return requests.request("GET", endpoint, headers=self.headers, params=params)

    @staticmethod
    def parse_response(response):
        return json.loads(response.content.decode("utf-8"))


if __name__ == "__main__":
    pass
