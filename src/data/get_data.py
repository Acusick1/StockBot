import pandas as pd
from pathlib import Path
from typing import Optional, Dict
from datetime import datetime, timedelta
from src.data.apis import FinanceApi
from src.data.database import get_interval_filename
from src.settings import VALID_PERIODS, VALID_INTERVALS, TIME_IN_SECONDS, DATA_PATH
from src.gen import keys_in_hdf

# TODO: Ideally update_data_file finds the last saved entry, downloads proceeding data and appends it to HDF5 file,
#  rather than reading in full dataset, combining and dropping duplicates. Generally, need to look into how pandas can
#  work with HDF5 files.


def get_stock_data(stocks, interval: VALID_INTERVALS, period: VALID_PERIODS, **kwargs) -> Dict:

    api = FinanceApi(interval=interval, period=period, **kwargs)

    data = {}
    file_path = DATA_PATH / get_interval_filename(interval)

    saved_keys = keys_in_hdf(file_path) if file_path.exists() else None

    for stock in stocks:
        if saved_keys is not None and stock in saved_keys:
            df = pd.DataFrame(pd.read_hdf(file_path, key=stock))

            start = kwargs.get("start")
            end = kwargs.get("end")

            if start is not None:

                data[stock] = get_start_end_data(df, start, end)
            else:
                data[stock] = get_period_data(df, period)

        if stock not in data or data[stock] is None:
            d = api.download_data([stock])
            data.update(d)

    return data


def get_start_end_data(df: pd.DataFrame, start: str, end: str) -> Optional[pd.DataFrame]:

    needed_dates = pd.date_range(start=start, end=end, freq="B")
    # Data only stored up to (not including) end date, so remove last date if more than one date requested
    if len(needed_dates) > 1:
        needed_dates = needed_dates.drop(needed_dates[-1])

    unique_dates = pd.Series([d.date() for d in df.index]).unique()

    if all([nd.date() in unique_dates for nd in needed_dates]):
        return df[start:end]


def get_period_data(df: pd.DataFrame, period: VALID_PERIODS) -> Optional[pd.DataFrame]:

    database_delta = df.index[-1] - df.index[0]
    period_seconds = TIME_IN_SECONDS[period]

    if database_delta.total_seconds() >= period_seconds:
        period_delta = timedelta(seconds=period_seconds)
        start = df.index[-1] - period_delta
        return df[start:]


def get_example_data():

    interval = "1m"
    period = "1d"
    api = FinanceApi(period=period, interval=interval)
    data = get_stock_data(["AAPL", "F"], period=period, interval=interval, api=api)
    return data


def update_data_file(interval: VALID_INTERVALS = "1d", file_path: Path = None):

    """Update data file with data up to current day"""
    if file_path is None:
        file_path = DATA_PATH / get_interval_filename(interval)

    if file_path.exists():
        saved_keys = keys_in_hdf(file_path)

        for stock in saved_keys:
            df = pd.DataFrame(pd.read_hdf(file_path, key=stock))
            start = df.index[-1] + timedelta(days=1)
            end = datetime.now(tz=start.tz)

            if (end - start).total_seconds() > TIME_IN_SECONDS[interval]:
                api = FinanceApi(start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"))
                api.download_data([stock], save=True)
    else:
        raise FileNotFoundError(f"No existing file found to update. File path: {file_path}")


if __name__ == "__main__":

    inter = "5m"
    get_stock_data(["MSFT", "F"], interval=inter, period="1d", start="2022-04-27", end="2022-05-03")
    update_data_file(interval=inter)
    get_example_data()
