from backtesting import Backtest
from strategies import daily
from src.db.main import DatabaseApi
from config import EXAMPLE_STOCKS


if __name__ == "__main__":

    api = DatabaseApi()
    data = api.request(stock=EXAMPLE_STOCKS, interval="1d", period="1y")

    for _, df in data.groupby(level=0, axis=1):

        df = df.droplevel(0, axis=1)
        df["Close"] = df["Adj Close"]
        bt = Backtest(df, daily.SmaCross, cash=10_000, commission=.002)

        # stats = bt.run()
        stats = bt.optimize(
            sma1=[5, 10, 15],
            sma2=[10, 20, 40],
            constraint=lambda p: p.sma1 < p.sma2
        )

        print(stats)
        bt.plot()
