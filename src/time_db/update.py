from pandarallel import pandarallel
from pandera import check_input
from pandera.typing import DataFrame
from sqlalchemy.orm import Session

from src.api.main import FinanceApi
from src.time_db import crud, models
from src.time_db.database import engine
from src.time_db.schemas import Daily, StockBase
from utils.gen import chunk

pandarallel.initialize()


@check_input(Daily.to_schema(), "data")
def insert_ohlc_data(ticker: str, data: DataFrame[Daily]):
    # Get the stock for this ticker
    stock = StockBase(ticker=ticker)
    stock_record = crud.get_record_by_filter(models.Stock, data=stock.model_dump())

    if not stock_record:
        # Create the stock if it does not exist
        stock_record = crud.create_record(models.Stock, data=stock.model_dump())

    crud.create_records(models.Daily, data.to_dict("records"))


def update_daily(stocks: list[str] | None = None):
    api = FinanceApi()

    if stocks is None:
        with Session(engine) as session:
            stocks = session.query(models.Stock).all()

        stocks = [s.ticker for s in stocks]

    for chunked in chunk(stocks, size=10):
        data = api.request(chunked, period="1y")

        for stock, stock_df in data.groupby(Daily.stock_id):
            stock_df = stock_df.dropna()

            if not stock_df.empty:
                insert_ohlc_data(ticker=stock, data=stock_df)


if __name__ == "__main__":
    update_daily(["MMM", "AAPL"])
