import pandas as pd
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
from backtesting.test import GOOG


class SmaCross(Strategy):
    # Define the two MA lags as *class variables*
    # for later optimization
    n1 = 10
    n2 = 20
    sma1 = None
    sma2 = None

    def init(self):
        # Precompute the two moving averages
        self.sma1 = self.I(simple_moving_avg, self.data.Close, self.n1)
        self.sma2 = self.I(simple_moving_avg, self.data.Close, self.n2)

    def next(self):
        # If sma1 crosses above sma2, close any existing
        # short trades, and buy the asset
        if crossover(self.sma1, self.sma2):
            self.position.close()
            self.buy()

        # Else, if sma1 crosses below sma2, close any existing
        # long trades, and sell the asset
        elif crossover(self.sma2, self.sma1):
            self.position.close()
            self.sell()


def simple_moving_avg(values, n):
    """
    Return simple moving average of values, at
    each step taking into account n previous values.
    """
    return pd.Series(values).rolling(n).mean()


if __name__ == "__main__":

    bt = Backtest(GOOG, SmaCross, cash=10_000, commission=.002)
    stats = bt.run()
    print(stats)
    bt.plot()
