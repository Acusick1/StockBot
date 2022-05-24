import os
import requests
import json
import h5py
import pandas as pd
from typing import Union, Dict, List, Tuple
from datetime import datetime
from src.gen import dataframe_from_dict

valid_periods = ("1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max")
valid_intervals = ("1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo")

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
    api = YahooApi()
    api.get_historical_data(stocks=["AAPL", "F"])
