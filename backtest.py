from backtesting import Backtest
from strategies.daily import SmaCross
from src.db import schemas
from src.db.main import DatabaseApi


if __name__ == "__main__":

    stocks = ["AAPL", "F", "AMD", "NVDA", "MSFT"]

    request = schemas.RequestBase(stock=stocks, interval="1d", period="1y", flat=False)

    api = DatabaseApi()
    data = api.get_data(request=request)

    for _, df in data.groupby(level=0, axis=1):

        df = df.droplevel(0, axis=1)
        bt = Backtest(df, SmaCross, cash=10_000, commission=.002)

        # stats = bt.run()
        stats = bt.optimize(
            sma1=[5, 10, 15],
            sma2=[10, 20, 40],
            constraint=lambda p: p.sma1 < p.sma2
        )

        print(stats)
        bt.plot()
