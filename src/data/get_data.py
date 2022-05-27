import pandas as pd
from typing import Optional, Dict
from datetime import datetime, timedelta
from src.data.apis import FinanceApi
from src.gen import create_h5_key, h5_key_elements, get_key_from_value, validate_strict_args
from src.settings import VALID_PERIODS, TIME_IN_SECONDS, STOCK_HISTORY_FILE, EXAMPLE_STOCKS

# TODO: Ideally update_data_file finds the last saved entry, downloads proceeding data and appends it to HDF5 file,
#  rather than reading in full dataset, combining and dropping duplicates. Generally, need to look into how pandas can
#  work with HDF5 files.

# TODO: get_start_end_data and get_period_data should be sub-functions of a get_data function in database.py.
#  But before this, potentially move to a SQL/NoSQL database if hdf5 not versatile enough.


def get_stock_data(stocks, api: FinanceApi) -> Dict:
    # TODO: Put in api class?
    interval = api.args["interval"]

    data = {}

    for stock in stocks:

        # check_database(stock)

        key = create_h5_key(interval, stock)

        with pd.HDFStore(STOCK_HISTORY_FILE) as h5:
            if key in h5.keys():
                df = pd.DataFrame(h5.get(key))

                period = api.args.get("period")
                start = api.args.get("start")
                end = api.args.get("end")

                if period is not None:
                    data[stock] = get_period_data(df, period)
                else:
                    data[stock] = get_start_end_data(df, start, end)

        if stock not in data or data[stock] is None:
            data[stock] = api.make_request([stock])

    return data


def get_start_end_data(df: pd.DataFrame, start: str, end: str) -> Optional[pd.DataFrame]:

    needed_dates = pd.date_range(start=start, end=end, freq="B")
    # Data only stored up to (not including) end date, so remove last date if more than one date requested
    if len(needed_dates) > 1:
        needed_dates = needed_dates.drop(needed_dates[-1])

    unique_dates = pd.Series([d.date() for d in df.index]).unique()

    if all([nd.date() in unique_dates for nd in needed_dates]):
        return df[start:end]


def get_period_data(df: pd.DataFrame, period: str) -> Optional[pd.DataFrame]:

    validate_strict_args(period, options=VALID_PERIODS, name="period")

    database_delta = df.index[-1] - df.index[0]
    period_seconds = TIME_IN_SECONDS[period]

    if database_delta.total_seconds() >= period_seconds:
        period_delta = timedelta(seconds=period_seconds)
        start = df.index[-1] - period_delta
        return df[start:]


def get_example_data():

    interval = "1m"
    period = "1d"
    api = FinanceApi(interval=interval, period=period)
    data = get_stock_data(["AAPL", "F"], api=api)
    return data


def get_latest_entry(h5_file: pd.HDFStore, key: str):

    return pd.DataFrame(h5_file.select(key, start=-1))


def update_data_file(h5_file: pd.HDFStore, key: str):

    """Update data file with data up to current day"""
    df = pd.DataFrame(h5_file.select(key, start=-2))
    start = df.index[-1] + timedelta(days=1)
    now = datetime.now(tz=start.tz)

    data_interval = (df.index[-1] - df.index[-2]).total_seconds()

    # Difference between today's date and first date to download must be greater than the interval of collected data,
    # and at least one day as API does not provide live data.
    if (now - start).total_seconds() > max(data_interval, TIME_IN_SECONDS["1d"]):
        download_key = h5_key_elements(key, index=-1)
        interval = get_key_from_value(TIME_IN_SECONDS, int(data_interval))
        api = FinanceApi(start=start.strftime("%Y-%m-%d"), end=now.strftime("%Y-%m-%d"), interval=interval)
        api.make_request([download_key], save=True)


if __name__ == "__main__":

    inter = "5m"
    per = "1d"
    keys = list(map(lambda x: create_h5_key(inter, x), EXAMPLE_STOCKS))
    api_obj = FinanceApi(start="2022-04-27", end="2022-05-03")

    get_stock_data(EXAMPLE_STOCKS, api=api_obj)

    for s, k in zip(EXAMPLE_STOCKS, keys):
        update_data_file(STOCK_HISTORY_FILE, key=k)

    get_example_data()
