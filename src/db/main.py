import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
from pathlib import Path
from typing import Optional
from datetime import datetime, timedelta
from src.api.main import FinanceApi
from src.db import schemas
from utils.hdf5 import h5_key_elements
from config import settings


class DatabaseApi:

    def __init__(self,
                 api: Optional[FinanceApi] = None,
                 market: str = "NYSE",
                 store_path: Path = settings.stock_history_file):

        self.api = api if api else FinanceApi()
        self.calendar = mcal.get_calendar(market)
        self.store = pd.HDFStore(str(store_path))

    def get_data(self, request: schemas.RequestBase, request_nan: bool = False):
        """
        Get data from an input request. Will first query internal database, if data is missing it will make an API call.
        
        Parameters
        ----------
        request: Request schema containing all necessary information to query database/finance api.
        request_nan: Whether or not to drop NaNs from data returned from database query, this guarantees an API request will be made if
            any value is NaN. The API data has many missing values, so this should only be used for infrequent or scheduled maintainence 
            calls.
        """

        # Taking a copy since we may make changes to stocks
        request = request.copy()

        base_indices = get_indices(request=request, calendar=self.calendar, frequency=request.get_base_interval())

        # Search slightly outwith bounds to ensure time is not excluding results
        start_date = request.start_date - timedelta(days=1)
        end_date = request.end_date + timedelta(days=1)

        data = {}
        diff = {}
        for tick in request.stock:

            key = request.get_h5_key(tick)
            db_data = self.get_db_data(key, start_date=start_date, end_date=end_date)

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
                    df = df.filter(items=diff[tick], axis=0)
                    self.store.append(key=request.get_h5_key(tick), value=df, format="table")

                mi = pd.concat([mi, response], axis=1)

        if mi is not None:
            mi = mi.filter(items=base_indices, axis=0).sort_index()

        return mi

    def get_db_data(self, key, start_date=None, end_date=None) -> Optional[pd.DataFrame]:
        """
        Filter store data by key and optional start/end dates
        """

        if key not in self.store.keys():
            return None

        where = []
        if start_date:
            where.append("index>=start_date")
        if end_date:
            where.append("index<=end_date")

        db_data = pd.DataFrame(self.store.select(key, where=where)).sort_index()

        return db_data

    def __del__(self):

        self.store.close()


def get_indices(request: schemas.RequestBase, calendar, frequency: Optional[str] = None):

    if frequency is None:
        frequency = request.interval.key

    if frequency.endswith("m"):
        schedule = calendar.schedule(start_date=request.start_date, end_date=request.end_date, tz=calendar.tz)
        indices = mcal.date_range(schedule, frequency=frequency)
    else:
        schedule = calendar.schedule(start_date=request.start_date, end_date=request.end_date, tz=None)
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
        request = schemas.RequestBase(stock=download_key, start_date=start, end_date=now, interval=interval)
        api.get_data(request=request)


def clean_data():
    """
    Cleaning every dataset by sorting indices and dropping duplicates
    """
    with pd.HDFStore(settings.stock_history_file) as h5:

        for key in h5.keys():

            df = pd.DataFrame(h5.get(key))
            df = df.drop_duplicates().sort_index()

            h5.put(key=key, value=df)


def create_fake_data(request: schemas.RequestBase, market: str = "NYSE"):

    calendar = mcal.get_calendar(market)
    index = get_indices(request=request, calendar=calendar)

    columns = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]

    data = np.random.rand(index.shape[0], len(columns))
    return pd.DataFrame(data, columns=columns, index=index)
