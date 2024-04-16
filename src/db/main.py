from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
from pandera import check_output
from pandera.typing import DataFrame

from config import settings
from src.api.main import FinanceApi
from src.db import schemas
from src.time_db.database import engine
from src.time_db.schemas import Daily, nytz
from src.time_db.update import insert_ohlc_data
from utils.gen import chunk, get_empty_pandera_df


class DatabaseApi:
    def __init__(
        self,
        api: FinanceApi | None = None,
        market: str = "NYSE",
        store_path: Path = settings.stock_history_file,
    ):
        self.api = api if api else FinanceApi()
        self.market = market
        self.calendar = mcal.get_calendar(market)
        self.store = pd.HDFStore(str(store_path))

    def request(
        self,
        stock: list[str],
        interval="1d",
        period="1y",
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        flat: bool = False,
        *args,
        **kwargs,
    ):
        """
        Wrapper around request creation and get_data to avoid making request pre-api call.

        See RequestBase in schemas in for parameter definitions.
        Args and kwargs pass onto get_data.
        """

        req = schemas.RequestBase(
            stock=stock,
            interval=interval,
            period=period,
            start_date=start_date,
            end_date=end_date,
        )
        data = self.get_data(req, *args, **kwargs)

        if len(stock) == 1 and flat:
            data = data.droplevel(0, axis=1)

        return data

    @check_output(Daily.to_schema())
    def get_data(
        self,
        request: schemas.RequestBase,
        request_nan: bool = False,
        force: bool = False,
    ):
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
        request = request.model_copy()

        base_indices = get_indices(
            request=request,
            calendar=self.calendar,
            frequency=request.get_base_interval(),
        ).astype(nytz)

        db_data = None

        if not force:
            tickers_string = ", ".join(f"'{ticker}'" for ticker in request.stock)
            # Assuming all requests have been converted to start date, end date format
            if request.start_date:
                query = f"""
                    SELECT *
                    FROM daily
                    WHERE timestamp BETWEEN '{request.start_date.date()}' AND '{request.end_date.date()}'
                    AND stock_id IN ({tickers_string})
                    ORDER BY timestamp;
                """  # noqa: S608
            else:
                query = f"""
                    SELECT *
                    FROM daily
                    AND stock_id IN ({tickers_string})
                    ORDER BY timestamp;
                """  # noqa: S608

            # TODO: Move to own validated function
            db_data = pd.read_sql(query, engine)
            db_data[Daily.timestamp] = db_data[Daily.timestamp].astype(nytz)

        request.stock = list(set(request.stock) - set(db_data[Daily.stock_id].unique()))

        if request.stock:
            response = self.api.make_request(request, interval_key=request.get_base_interval())

            # Ensure all rows are present.
            # Data returned may not be complete, and we do not want to make repeated requests because data is missing.
            # TODO: This can be done cleaner, look up multiple operations on a dataframe
            response = (
                response.set_index(Daily.timestamp)
                .reindex(base_indices)
                .reset_index()
                .rename(columns={"index": Daily.timestamp})
            )

            if response is not None and response.shape[0]:
                self.put_data(response)

            db_data = pd.concat([db_data, response]).reset_index(drop=True)

        return db_data

    def get_db_data(self, key, start_date=None, end_date=None) -> pd.DataFrame | None:
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

        return pd.DataFrame(self.store.select(key, where=where)).sort_index()

    def put_data(self, data: DataFrame[Daily]):
        """
        Insert data to database
        """
        for stock, stock_df in data.groupby(Daily.stock_id):
            stock_df = stock_df.dropna()

            if not stock_df.empty:
                insert_ohlc_data(ticker=stock, data=stock_df)

    def update_daily(self, tickers: list[str] | None = None, *args, **kwargs):
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
        return [k.split("/")[-1] for k in self.store.keys() if group in k]

    def __del__(self):
        self.store.close()


def get_indices(request: schemas.RequestBase, calendar, frequency: str | None = None):
    if frequency is None:
        frequency = request.interval.key

    if frequency.endswith("m"):
        schedule = calendar.schedule(start_date=request.start_date, end_date=request.end_date, tz=calendar.tz)
        indices = mcal.date_range(schedule, frequency=frequency)
    else:
        schedule = calendar.schedule(start_date=request.start_date, end_date=request.end_date, tz=None)
        indices = schedule.index.tz_localize(calendar.tz)

    return indices


# TODO: Solution to market tz and custom nytz (needed for pandera)
@check_output(Daily.to_schema())
def create_fake_data(request: schemas.RequestBase, market: str = "NYSE") -> DataFrame[Daily]:
    calendar = mcal.get_calendar(market)
    timestamps = get_indices(request=request, calendar=calendar).astype(nytz)

    daily_df = get_empty_pandera_df(Daily)

    # Select columns before doing anything, somehow int columns can change to float even when working on a different
    # column
    int_cols = daily_df.select_dtypes(int).columns
    float_cols = daily_df.select_dtypes(float).columns

    daily_df[Daily.timestamp] = timestamps
    daily_df[Daily.stock_id] = "A"

    daily_df[int_cols] = np.random.randint(0, 100, size=daily_df[int_cols].shape)  # noqa: NPY002
    daily_df[float_cols] = np.random.rand(*daily_df[float_cols].shape)  # noqa: NPY002

    return daily_df
