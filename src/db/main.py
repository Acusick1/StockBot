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

    def get_data(self, request: schemas.RequestBase):

        indices = get_indices(request=request, calendar=self.calendar)

        data = {}
        diff = {}
        rm_stock = []
        # TODO: Lots of refactoring and abstraction

        for tick, key in zip(request.stock, request.get_h5_keys()):

            diff[tick] = indices

            if key in self.store.keys():

                # Search slightly outwith bounds to ensure time is not excluding results
                start_date = request.start_date - timedelta(days=1)
                end_date = request.end_date + timedelta(days=1)
                db_data = self.get_db_data(key, start_date=start_date, end_date=end_date)

                # Lots of missing ticks in minute based raw data, so cannot directly compare indices, have to
                # compare dates instead
                diff_dates = set(indices.date) - set(db_data.index.date)

                if diff_dates:
                    # Now take difference at lowest level (may still be dates only) to filter request data for
                    # database insertion
                    diff[tick] = indices.difference(db_data.index)

                else:
                    # All data present in database, no need to make request, can assign data directly
                    diff[tick] = None
                    data[tick] = db_data
                    rm_stock.append(tick)

        if len(rm_stock) != len(request.stock):

            request.stock = list(set(request.stock) - set(rm_stock))

            response = self.api.make_request(
                request,
                interval_key=request.get_base_interval()
            )

            if isinstance(response, pd.DataFrame):
                for tick, key in zip(request.stock, request.get_h5_keys()):
                    if response.shape[0]:

                        # TODO: This may have been a database error, commenting out until confirmed
                        # For some reason the API can return duplicated rows and duplicated indices containing
                        # different data. In the first case drop_duplicates will suffice, but for the second it has
                        # to be decided which data should be kept
                        # response = response[~response.index.duplicated(keep='first')]

                        data[tick] = response
                        put_data = response.filter(items=diff[tick], axis=0)
                        self.store.append(key=key, value=put_data, format="table")
            else:
                pass

        data = {tick: v.filter(items=indices, axis=0).sort_index().dropna() for tick, v in data.items()}

        return data

    def get_db_data(self, key, start_date=None, end_date=None) -> pd.DataFrame:
        """
        Filter store data by key and optional start/end dates
        """

        where = []
        if start_date:
            where.append("index>=start_date")
        if end_date:
            where.append("index<=end_date")

        db_data = pd.DataFrame(self.store.select(key, where=where))

        return db_data

    def __del__(self):

        self.store.close()


def get_indices(request: schemas.RequestBase, calendar):

    base_interval = request.get_base_interval()

    if base_interval == "1m":
        schedule = calendar.schedule(start_date=request.start_date, end_date=request.end_date, tz=calendar.tz)
        indices = mcal.date_range(schedule, frequency=base_interval.upper())
    else:
        schedule = calendar.schedule(start_date=request.start_date, end_date=request.end_date, tz=None)
        indices = schedule.index

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
