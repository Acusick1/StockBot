import numpy as np
import pandas as pd
from typing import Optional
from datetime import datetime, timedelta
from src.api.main import FinanceApi
from src.db import schemas
from utils.hdf5 import h5_key_elements
from utils.gen import trading_day_range
from config import settings


class DatabaseApi:

    data_file = settings.stock_history_file

    def __init__(self, api: Optional[FinanceApi] = None):

        self.api = api if api else FinanceApi()

    def get_data(self, request: schemas.RequestBase):

        indices = trading_day_range(
            bday_start=request.start_date,
            bday_end=request.end_date,
            bday_freq=request.interval.dfreq,
            iday_freq=request.interval.ifreq,
        )

        data = {}
        db_data = {}
        diff = {}
        rm_stock = []
        # TODO: Lots of refactoring and abstraction
        with pd.HDFStore(self.data_file) as h5:

            for tick, key in zip(request.stock, request.get_h5_keys()):

                if key in h5.keys():
                    db_data[tick] = pd.DataFrame(
                        h5.select(key, where=["index>=request.start_date & index<=request.end_date"])
                    )

                    # TODO: Lots of missing ticks in raw data, need to do a more rough estimation than checking every
                    #  tick, e.g. one tick for each date when daily base, two or more for minute base.
                    diff[tick] = indices.difference(db_data[tick].index)
                else:
                    diff[tick] = indices

                # All data present in database, no need to make request
                if not diff[tick].shape[0]:
                    data[tick] = db_data[tick]
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

                            data[tick] = response
                            put_data = response.filter(items=diff[tick], axis=0)
                            h5.append(key=key, value=put_data, format="table")
                else:
                    pass

        data = {tick: v.filter(items=indices, axis=0).sort_index().dropna() for tick, v in data.items()}

        return data


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


def create_fake_data(request: schemas.RequestBase):

    index = trading_day_range(
        bday_start=request.start_date,
        bday_end=request.end_date,
        iday_freq=request.interval.ifreq
    )

    columns = ["Open", "Close", "Adj Close", "Volume"]

    data = np.random.rand(index.shape[0], len(columns))
    return pd.DataFrame(data, columns=columns, index=index)
