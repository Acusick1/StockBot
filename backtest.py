from backtesting import Backtest
from strategies.daily import SmaCross
from src.db import schemas
from src.db.main import DatabaseApi


if __name__ == "__main__":

    stocks = ["AAPL", "F", "AMD", "NVDA", "MSFT"]

    request = schemas.RequestBase(stock=stocks, end_date="2022-10-01", interval="1d", period="1y")

    api = DatabaseApi()
    data = api.get_data(request=request)

    for _, d in data.groupby(level=0, axis=1):
        d = d.droplevel(0, axis=1)
        bt = Backtest(d, SmaCross, cash=10_000, commission=.002)
        stats = bt.run()
        print(stats)
        bt.plot()
