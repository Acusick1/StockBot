import pandas as pd
from typing import Optional, Union, List, Tuple
from datetime import datetime, timedelta
from src.data.apis import FinanceApi
from utils.hdf5 import create_h5_key, h5_key_elements
from utils.gen import validate_strict_args, trading_day_range
from src.settings import VALID_PERIODS, VALID_INTERVALS, STOCK_HISTORY_FILE, EXAMPLE_STOCKS, TIME_MAPPINGS, Interval


class DatabaseApi:

    # TODO: Init here or variable required elsewhere?
    data_file = STOCK_HISTORY_FILE
    date_fmt = "%Y-%m-%d"

    def __init__(self,
                 api: FinanceApi,
                 interval: Interval = TIME_MAPPINGS["1d"],
                 period: Optional[str] = None,
                 start: Optional[str] = None,
                 end: Optional[str] = None,
                 ):

        self.api = api
        self.interval = interval
        self.period = period
        self.start = start
        self.end = end

        self.validate_request()

    def get_data(self, tickers: Union[List, Tuple], save=True):

        data = {}

        for tick in tickers:

            key = create_h5_key(self.interval.base.key, tick)
            data[tick] = self.check_database(key)

            if data[tick] is None:
                response = self.api.make_request(
                    [tick],
                    interval=self.interval.base.value,
                    period=self.period,
                    start=self.start,
                    end=self.end)

                if save:
                    with pd.HDFStore(self.data_file) as h5:
                        merge_data(response[tick], h5_file=h5, key=key)

                # TODO: Calling database again since API is called with base interval and may need to be filtered.
                #  Filtering should therefore probably be done separately, not within get_period/start_end_data
                data[tick] = self.check_database(key)

        return data

    def check_database(self, key):

        with pd.HDFStore(self.data_file) as h5:
            if key in h5.keys():
                df = pd.DataFrame(h5.get(key))

                if self.period is not None:
                    data = self.get_period_data(df)
                else:
                    data = self.get_start_end_data(df)

                return data

    def get_start_end_data(self, df: pd.DataFrame, bdays=True) -> Optional[pd.DataFrame]:

        if bdays:
            needed_dates = pd.bdate_range(start=self.start, end=self.end, freq=self.interval.dfreq, inclusive="left")
        else:
            needed_dates = pd.date_range(start=self.start, end=self.end, freq=self.interval.dfreq, inclusive="left")

        # TODO: Changed with inclusive, validate expected behaviour
        # Data only stored up to (not including) end date, so remove last date if more than one date requested
        # if len(needed_dates) > 1:
        #    needed_dates = needed_dates.drop(needed_dates[-1])

        unique_dates = pd.Series([d.date() for d in df.index]).unique()

        if all([nd.date() in unique_dates for nd in needed_dates]):

            if self.interval.base.value == "1m":
                keep = trading_day_range(bday_start=self.start, bday_end=self.end, tz=df.index[0].tz)
            else:
                keep = needed_dates

            df = df.filter(items=keep, axis=0).dropna()

            return df

    def get_period_data(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        # TODO: Filter as in start_end_func (abstract)
        database_delta = df.index[-1] - df.index[0]
        period_seconds = TIME_MAPPINGS[self.period].delta.total_seconds()

        if database_delta.total_seconds() >= period_seconds:
            period_delta = timedelta(seconds=period_seconds)
            start = df.index[-1] - period_delta
            return df[start:]

    def validate_request(self):
        # TODO: Does validation interval.base make sense, and should intervals be wrapped in TIME_MAPPINGS?
        validate_strict_args(self.interval.base.value, options=VALID_INTERVALS, name="interval")
        validate_strict_args(self.period, options=VALID_PERIODS, name="period", optional=True)

        if self.period is not None:
            p = TIME_MAPPINGS[self.period].delta.total_seconds()
        else:
            p = (datetime.strptime(self.end, self.date_fmt) -
                 datetime.strptime(self.start, self.date_fmt)).total_seconds()

        assert p >= self.interval.delta.total_seconds() * 2


def get_example_data():

    interval = "1m"
    period = "1d"
    api = FinanceApi()
    database = DatabaseApi(interval=TIME_MAPPINGS[interval], period=period, api=api)
    data = database.get_data(["AAPL", "F"])
    return data


def get_latest_entry(h5_file: pd.HDFStore, key: str):

    return pd.DataFrame(h5_file.select(key, start=-1))


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
    database_obj = DatabaseApi(start="2022-05-20", end="2022-05-27", interval=TIME_MAPPINGS[inter], api=api_obj)
    database_obj.get_data(EXAMPLE_STOCKS)

    database_obj = DatabaseApi(start="2022-03-20", end="2022-05-27", interval=TIME_MAPPINGS["5d"], api=api_obj)
    database_obj.get_data(EXAMPLE_STOCKS)

    for s, k in zip(EXAMPLE_STOCKS, keys):
        update_data_file(STOCK_HISTORY_FILE, key=k)

    get_example_data()
