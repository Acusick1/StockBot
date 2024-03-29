import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
from pathlib import Path
from typing import Optional
from datetime import datetime, timedelta
from src.api.main import FinanceApi
from src.db import schemas
from utils.hdf5 import h5_key_elements
from utils.gen import chunk
from config import settings


class DatabaseApi:

    def __init__(self,
                 api: Optional[FinanceApi] = None,
                 market: str = "NYSE",
                 store_path: Path = settings.stock_history_file):

        self.api = api if api else FinanceApi()
        self.market = market
        self.calendar = mcal.get_calendar(market)
        self.store = pd.HDFStore(str(store_path))

    def request(self, stock: list[str], interval="1d", period="1y", start_date: Optional[datetime] = None, end_date: Optional[datetime] = None, flat: bool = False, *args, **kwargs):
        """
        Wrapper around request creation and get_data to avoid making request pre-api call.

        See RequestBase in schemas in for parameter definitions.
        Args and kwargs pass onto get_data.
        """

        req = schemas.RequestBase(stock=stock, interval=interval,
                                  period=period, start_date=start_date, end_date=end_date)
        data = self.get_data(req, *args, **kwargs)

        if len(stock) == 1 and flat:
            data = data.droplevel(0, axis=1)

        return data

    def get_data(self, request: schemas.RequestBase, request_nan: bool = False, force: bool = False):
        """
        Get data from an input request. Will first query internal database, if data is missing it will make an API call.

        Parameters
        ----------
        request: Request schema containing all necessary information to query database/finance api.
        request_nan: Whether to drop NaNs from data returned from database query, this guarantees an API request will be
            made if any value is NaN. The API data has many missing values, so this should only be used for infrequent
            or scheduled maintenance calls.
        force: Make API call without checking internal database. Useful for fixing broken data.
        """

        # Taking a copy since we may make changes to stocks
        request = request.copy()

        base_indices = get_indices(
            request=request, calendar=self.calendar, frequency=request.get_base_interval())

        data = {}
        diff = {}

        if not force:
            # Search slightly outwith bounds to ensure time is not excluding results
            start_date = request.start_date - timedelta(days=1)
            end_date = request.end_date + timedelta(days=1)

            for tick in request.stock:

                key = request.get_h5_key(tick)
                db_data = self.get_db_data(
                    key, start_date=start_date, end_date=end_date)

                if db_data is None or not db_data.shape[0]:
                    diff[tick] = base_indices
                    continue

                # Drop NaNs before checking for differences
                if request_nan:
                    db_data = db_data.dropna()

                # Lots of missing ticks in minute based raw data, so cannot directly compare indices, have to
                # compare dates instead
                diff_dates = set(base_indices.date) - set(db_data.index.date)

                if diff_dates:
                    # Now take difference at lowest level (may still be dates only) to filter request data for
                    # database insertion
                    diff[tick] = base_indices.difference(db_data.index)
                else:
                    # All data present in database, no need to make request, can assign data directly
                    data[tick] = db_data

        mi = pd.concat(data, axis=1) if data else None
        request.stock = list(set(request.stock) - set(data.keys()))

        if request.stock:

            response = self.api.make_request(
                request,
                interval_key=request.get_base_interval()
            )

            # Ensure all rows are present.
            # Data returned may not be complete, and we do not want to make repeated requests because data is missing.
            response = response.reindex(base_indices)

            if response.shape[0]:

                # Make multi-index
                if response.columns.nlevels == 1:
                    response = pd.concat({request.stock[0]: response}, axis=1)

                for tick, df in response.groupby(level=0, axis=1):

                    tick = str(tick)
                    df = df.droplevel(0, axis=1)

                    if tick in diff:
                        df = df.filter(items=diff[tick], axis=0)
                    
                    # Joining, keeping fresh data, and sorting
                    # Reading, merging and overwriting stored data here may take a while, but appending can be error prone.
                    #   At least this way the duplicates and sorting is done immediately.
                    key = request.get_h5_key(tick)
                    if key in self.store.keys():
                        df = pd.concat([self.store.get(key), df])
                    
                    df = df[~df.index.duplicated(keep="last")]
                    df = df.sort_index()

                    self.store.put(key=key, value=df, format="table")

                mi = pd.concat([mi, response], axis=1)

        if mi is not None:
            mi = mi.filter(items=base_indices, axis=0)

        return mi

    def get_db_data(self, key, start_date=None, end_date=None) -> Optional[pd.DataFrame]:
        """
        Filter store data by key and optional start/end dates
        NOTE: Currently assumes full days only, inclusive of start/end dates
        """

        if key not in self.store.keys():
            return None

        where = []
        if start_date:
            where.append("index>=start_date")
        if end_date:
            # Make datetime midnight (i.e. 00:00 on following day)
            end_date += timedelta(1)
            end_date = end_date.replace(hour=0, minute=0)
            where.append("index<=end_date")

        db_data = pd.DataFrame(self.store.select(
            key, where=where)).sort_index()

        return db_data
    
    def update_daily(self, tickers: Optional[list[str]] = None, *args, **kwargs):
        """
        Update database based on tickers currently in database (default) or input tickers

        Parameters
        ----------
        tickers: Stock tickers to update/add to database (optional: default is to update existing)
        kw/args: Additional parameters to get_data method
        """

        if tickers is None:
            tickers = self.get_stored_tickers(group="daily")

        chunked_tickers = chunk(tickers, size=self.api.max_stocks_per_request)
        for group in chunked_tickers:

            req = schemas.RequestBase(stock=group)
            _ = self.get_data(req, *args, **kwargs)

    def get_stored_tickers(self, group: str = "daily"):

        tickers = [k.split("/")[-1] for k in self.store.keys() if group in k]
        return tickers

    def __del__(self):

        self.store.close()


def get_indices(request: schemas.RequestBase, calendar, frequency: Optional[str] = None):

    if frequency is None:
        frequency = request.interval.key

    if frequency.endswith("m"):
        schedule = calendar.schedule(
            start_date=request.start_date, end_date=request.end_date, tz=calendar.tz)
        indices = mcal.date_range(schedule, frequency=frequency)
    else:
        schedule = calendar.schedule(
            start_date=request.start_date, end_date=request.end_date, tz=None)
        indices = schedule.index.tz_localize(calendar.tz)

    return indices


def update_data_file(h5_file: pd.HDFStore, key: str, interval="1d"):
    """
    Update data file with data up to current day
    """
    api = DatabaseApi()

    df = pd.DataFrame(h5_file.select(key, start=-2))
    start = df.index[-1] + timedelta(days=1)
    now = datetime.now(tz=start.tz)

    data_interval = (df.index[-1] - df.index[-2]).total_seconds()

    # Difference between today's date and first date to download must be greater than the interval of collected data,
    # and at least one day as API does not provide live data.
    if (now - start).total_seconds() > max(data_interval, timedelta(days=1).total_seconds()):
        download_key = h5_key_elements(key, index=-1)
        request = schemas.RequestBase(
            stock=download_key, start_date=start, end_date=now, interval=interval)
        api.get_data(request=request)


def clean_data():
    """
    Cleaning every dataset by sorting indices and dropping duplicates
    """
    with pd.HDFStore(settings.stock_history_file) as h5:

        for key in h5.keys():

            df = pd.DataFrame(h5.get(key))
            df = df.drop_duplicates().sort_index()

            h5.put(key=key, value=df, format="table")


def create_fake_data(request: schemas.RequestBase, market: str = "NYSE"):

    calendar = mcal.get_calendar(market)
    index = get_indices(request=request, calendar=calendar)

    columns = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]

    data = np.random.rand(index.shape[0], len(columns))
    return pd.DataFrame(data, columns=columns, index=index)
