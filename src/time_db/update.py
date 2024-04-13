import pandas as pd
from functools import partial
from pandarallel import pandarallel
from sqlalchemy.orm import Session
from src.time_db.database import engine
from src.api.main import FinanceApi
from src.time_db import crud, models, schemas
from utils.gen import chunk


pandarallel.initialize()


def insert_olhc_data(ticker: str, data: pd.DataFrame):
    # Get the stock for this ticker
    stock = schemas.StockBase(ticker=ticker)
    stock_record = crud.get_record_by_filter(models.Stock, data=stock.dict())

    if not stock_record:
        # Create the stock if it does not exist
        stock_record = crud.create_record(models.Stock, data=stock.dict())

    daily_data = data.parallel_apply(
        partial(create_daily, ticker=ticker, as_dict=True), axis=1
    ).tolist()
    crud.create_records(models.Daily, daily_data)


def create_daily(row: pd.Series, ticker: str, as_dict: bool = True):
    daily = schemas.DailyBase(
        stock_id=ticker,
        timestamp=row.name,
        open=row["Open"],
        high=row["High"],
        low=row["Low"],
        close=row["Close"],
        adj_close=row["Adj Close"],
        volume=row["Volume"],
    )
    return daily.dict() if as_dict else daily


def update_daily():
    api = FinanceApi()

    with Session(engine) as session:
        stocks = session.query(models.Stock).all()

    stocks = [s.ticker for s in stocks]

    for chunked in chunk(stocks, size=10):
        data = api.request(chunked, period="1y")

        for stock, df in data.groupby(level=0, axis=1):
            df = df.droplevel(0, axis=1)
            df = df.dropna()

            if not df.empty:
                insert_olhc_data(ticker=stock, data=df)


if __name__ == "__main__":
    update_daily()
