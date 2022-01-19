import os
import requests
import json
import h5py
import yfinance as yf
import pandas as pd
from datetime import datetime
from collections import abc
from src.gen import dataframe_from_dict

YAHOO_API_KEY = os.environ.get('YAHOO_FINANCE_API_KEY')
YAHOO_API_URL = "https://yfapi.net/v8/finance/spark"

EXAMPLE_STOCKS = ['AAPL', 'F']

# TODO: Think about how to properly integrate api with different options,
#  local variables with some defaults probably not best way to do this
PERIOD_QUERY = {
    "symbols": "",
    "interval": "1d",
    "range": "10y"}

FIXED_QUERY = {
    "symbols": "",
    "start": "2020-01-01",
    "end": "2020-12-31"}

HEADERS = {'x-api-key': YAHOO_API_KEY}

# TODO: is yfinance and api data consistent?


def get_example_fixed_data():
    return download_fixed_data(EXAMPLE_STOCKS)


def download_fixed_data(stocks):
    return yf.download(stocks, start=FIXED_QUERY["start"], end=FIXED_QUERY["end"])


def download_period_data(stocks):
    return yf.download(stocks, period=PERIOD_QUERY["range"], interval=PERIOD_QUERY["interval"])


def get_historical_data(stocks):

    now = datetime.now()
    filename = "data_" + now.strftime("%d_%b_%Y") + '.hdf5'

    with h5py.File(filename, 'a') as f:
        saved_keys = list(f.keys())
        print(saved_keys)

    dfs = {s: pd.read_hdf(filename, key=s) for s in stocks if s in saved_keys}
    stocks = [s for s in stocks if s not in dfs.keys()]

    if stocks:
        content = yahoo_api_call(stocks)
        for stock, data in content.items():
            data['timestamp'] = pd.to_datetime(data['timestamp'], unit='s')
            df = dataframe_from_dict(data)
            df.set_index('timestamp', inplace=True)
            dfs[stock] = df

            df.to_hdf(filename, key=stock, mode='a')

    return dfs


def yahoo_api_call(stocks: abc.Iterable):

    PERIOD_QUERY['symbols'] = ','.join(stocks)
    response = requests.request("GET", YAHOO_API_URL, headers=HEADERS, params=PERIOD_QUERY)
    content = json.loads(response.content.decode('utf-8'))

    return content
