import pandas as pd
from typing import Optional, Union, List, Tuple
from datetime import datetime, timedelta
from src.data.apis import FinanceApi
from utils.hdf5 import create_h5_key, h5_key_elements
from utils.gen import trading_day_range
from src.settings import STOCK_HISTORY_FILE, EXAMPLE_STOCKS, TIME_MAPPINGS, Interval


class DatabaseApi:

    # TODO: Init here or variable required elsewhere?
    data_file = STOCK_HISTORY_FILE
    date_fmt = "%Y-%m-%d"

    def __init__(self,
                 api: FinanceApi,
                 interval: Interval = TIME_MAPPINGS["1d"],
                 period: Optional[Interval] = None,
                 start: Optional[str] = None,
                 end: Optional[str] = None,
                 ):

        self.api = api
        self.interval = interval
        # self.period = period
        self.start = start if start is None else datetime.strptime(start, self.date_fmt).date()
        self.end = end if end is None else datetime.strptime(end, self.date_fmt).date()

        if period is not None:
            self.period_to_dates(period)

        self.clean_dates()
        self.validate_request()

    def period_to_dates(self, period):

        delta = period.delta

        if self.start is not None:
            self.end = self.start + delta
        else:
            if self.end is None:
                self.end = datetime.today().date()

            self.start = self.end - delta

    def clean_dates(self):

        # Using yesterday as max end time since data does not update live
        max_end_time = datetime.today().date() - timedelta(days=1)

        if self.end > max_end_time:
            self.end = max_end_time

        # If start date is a weekend, set to following Monday
        if self.start.isoweekday() in {6, 7}:
            self.start += timedelta(days=8 - self.start.isoweekday())

        # If end date is a weekend, set to previous Friday
        if self.end.isoweekday() in {6, 7}:
            self.end -= timedelta(days=self.end.isoweekday() - 5)

    def get_data(self, tickers: Union[List, Tuple]):

        data = {}

        for tick in tickers:

            key = create_h5_key(self.interval.base.key, tick)
            failures = (True, True)

            with pd.HDFStore(self.data_file) as h5:
                if key in h5.keys():

                    first = pd.DataFrame(h5.select(key, stop=1)).index[0].date()
                    last = pd.DataFrame(h5.select(key, start=-1)).index[0].date()

                    failures = (first > self.start, last < self.end)

                if any(failures):

                    start = self.start
                    end = self.end

                    if all(failures):
                        pass
                    elif failures[0]:
                        end = first
                    elif failures[1]:
                        start = last + timedelta(days=1)

                    response = self.api.make_request(
                        [tick],
                        interval=self.interval.base.value,
                        start=start.strftime(self.date_fmt),
                        end=(end + timedelta(days=1)).strftime(self.date_fmt)
                    )

                    if failures[1]:
                        h5.append(key, response[tick])
                    else:
                        merge_data(response[tick], h5_file=h5, key=key)

                df = pd.DataFrame(h5.select(key, where=["index>=self.start & index<=self.end"]))

                if self.interval.dfreq in ["B", None] and self.interval.ifreq in ["T", None]:
                    data[tick] = df
                else:
                    data[tick] = self.filter_data(df)

        return data

    def filter_data(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:

        keep = trading_day_range(
            bday_start=self.start,
            bday_end=self.end,
            bday_freq=self.interval.dfreq,
            iday_freq=self.interval.ifreq,
            tz=df.index[0].tz
        )

        df = df.filter(items=keep, axis=0).dropna()

        return df

    def validate_request(self):

        # If interval is minutes/hours then this will always be true, but have to skip as adding delta will change
        # right-hand side from date to datetime.
        if not any((self.interval.delta.minutes, self.interval.delta.hours)):
            assert self.end > self.start + self.interval.delta


def get_example_data():

    interval = "1m"
    period = "1d"
    api = FinanceApi()
    database = DatabaseApi(interval=TIME_MAPPINGS[interval], period=TIME_MAPPINGS[period], api=api)
    data = database.get_data(["AAPL", "F"])
    return data


def get_latest_entry(h5_file: pd.HDFStore, key: str):

    return pd.DataFrame(h5_file.select(key, start=-1))


def append_data(df: pd.DataFrame, h5_file: pd.HDFStore, key: str):

    h5_file.append(key, df)


def merge_data(df: pd.DataFrame, h5_file: pd.HDFStore, key: str):

    stored_data = h5_file.get(key) if key in h5_file.keys() else None

    if stored_data is not None:
        df = pd.concat([stored_data, df]).drop_duplicates().sort_index()

    h5_file.put(key, df, format="table")


def update_data_file(h5_file: pd.HDFStore, key: str, interval="1d"):
    # TODO: Fix after update
    """Update data file with data up to current day"""
    df = pd.DataFrame(h5_file.select(key, start=-2))
    start = df.index[-1] + timedelta(days=1)
    now = datetime.now(tz=start.tz)

    data_interval = (df.index[-1] - df.index[-2]).total_seconds()

    # Difference between today's date and first date to download must be greater than the interval of collected data,
    # and at least one day as API does not provide live data.
    if (now - start).total_seconds() > max(data_interval, TIME_MAPPINGS["1d"].delta.total_seconds()):
        download_key = h5_key_elements(key, index=-1)
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

    database_obj = DatabaseApi(period=TIME_MAPPINGS["1mo"], interval=TIME_MAPPINGS["1d"], api=api_obj)
    database_obj.get_data(EXAMPLE_STOCKS)

    database_obj = DatabaseApi(start="2022-05-20", end="2022-05-27", interval=TIME_MAPPINGS[inter], api=api_obj)
    database_obj.get_data(EXAMPLE_STOCKS)

    database_obj = DatabaseApi(start="2022-03-20", end="2022-05-27", interval=TIME_MAPPINGS["5d"], api=api_obj)
    database_obj.get_data(EXAMPLE_STOCKS)

    for s, k in zip(EXAMPLE_STOCKS, keys):
        update_data_file(STOCK_HISTORY_FILE, key=k)

    get_example_data()
