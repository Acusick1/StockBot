import pandas as pd
from typing import Optional, Union, List, Tuple
from datetime import datetime, timedelta
from src.data.apis import FinanceApi
from utils.hdf5 import create_h5_key, h5_key_elements
from utils.gen import get_key_from_value, validate_strict_args
from src.settings import VALID_PERIODS, TIME_IN_SECONDS, STOCK_HISTORY_FILE, EXAMPLE_STOCKS


class DatabaseApi:

    # TODO: Init here or variable required elsewhere?
    data_file = STOCK_HISTORY_FILE

    def __init__(self,
                 api: FinanceApi,
                 interval: str = "1d",
                 period: Optional[str] = None,
                 start: Optional[str] = None,
                 end: Optional[str] = None,
                 ):

        self.api = api
        self.interval = interval
        self.period = period
        self.start = start
        self.end = end

    def get_data(self, tickers: Union[List, Tuple]):

        data = {}

        for tick in tickers:

            data[tick] = self.check_database(tick)

            if data[tick] is None:
                data.update(
                    self.api.make_request([tick],
                                          interval=self.interval,
                                          period=self.period,
                                          start=self.start,
                                          end=self.end)
                )

        return data

    def check_database(self, ticker):

        key = create_h5_key(self.interval, ticker)

        with pd.HDFStore(self.data_file) as h5:
            if key in h5.keys():
                df = pd.DataFrame(h5.get(key))

                if self.period is not None:
                    data = get_period_data(df, self.period)
                else:
                    data = get_start_end_data(df, self.start, self.end)

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
    api = FinanceApi()
    database = DatabaseApi(interval=interval, period=period, api=api)
    data = database.get_data(["AAPL", "F"])
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


def clean_data():

    with pd.HDFStore(STOCK_HISTORY_FILE) as h5:

        for key in h5.keys():

            df = pd.DataFrame(h5.get(key))
            df = df.drop_duplicates().sort_index()

            # TODO: Could download data again, but should this be done here or just removed?
            # Have to drop duplicate indices, no way to know which is truth
            df = df[~df.index.duplicated(False)]

            h5.put(key=key, value=df)


if __name__ == "__main__":

    inter = "5m"
    per = "1d"
    keys = list(map(lambda x: create_h5_key(inter, x), EXAMPLE_STOCKS))
    api_obj = FinanceApi()
    database_obj = DatabaseApi(start="2022-04-27", end="2022-05-03", api=api_obj)
    database_obj.get_data(EXAMPLE_STOCKS)

    for s, k in zip(EXAMPLE_STOCKS, keys):
        update_data_file(STOCK_HISTORY_FILE, key=k)

    get_example_data()
