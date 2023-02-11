import pandas as pd
from backtesting import Backtest, Strategy
from backtesting.lib import crossover, TrailingStrategy
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


class MacdSignalCross(TrailingStrategy):

    macd = None
    singal = None

    def init(self):
        # Precompute macd and signal
        self.macd, self.signal = self.I(macd, self.data.Close)
        self.set_trailing_sl(0.0001)

    def next(self):
        # If sma1 crosses above sma2, close any existing
        # short trades, and buy the asset
        if crossover(self.macd, self.signal):
            self.position.close()
            self.buy()

        # Else, if sma1 crosses below sma2, close any existing
        # long trades, and sell the asset
        elif crossover(self.signal, self.macd):
            self.position.close()
            self.sell()


def simple_moving_avg(values, n):
    """
    Return simple moving average of values, at
    each step taking into account n previous values.
    """
    return pd.Series(values).rolling(n).mean()


def macd(values, n_fast: int = 12, n_slow: int = 26, smooth: int = 9):
    """
    Return MACD (Moving Average Convergence/Divergence)
    using fast and slow exponential moving averages.
    """
    emaslow = pd.Series(values).ewm(span=n_slow, adjust=False).mean()
    emafast = pd.Series(values).ewm(span=n_fast, adjust=False).mean()
    macd = emafast - emaslow
    signal = macd.ewm(span=smooth, adjust=False).mean()

    return macd, signal


if __name__ == "__main__":

    bt = Backtest(GOOG, SmaCross, cash=10_000, commission=.002)
    stats = bt.run()
    print(stats)
    bt.plot()
