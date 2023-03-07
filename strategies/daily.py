import numpy as np
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
        super().init()

        # Precompute the two moving averages
        self.sma1 = self.I(simple_moving_avg, self.data.Close, self.n1)
        self.sma2 = self.I(simple_moving_avg, self.data.Close, self.n2)

    def next(self):
        super().next()

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
        super().init()

        # Precompute macd and signal
        self.macd, self.signal = self.I(macd_signal, self.data.Close)
        self.set_trailing_sl(2)

    def next(self):
        super().next()

        # If macd crosses above signal, close any existing
        # short trades, and buy the asset
        if crossover(self.macd, self.signal):
            self.position.close()
            self.buy()

        # Else, if macd crosses below signal, close any existing
        # long trades, and sell the asset
        elif crossover(self.signal, self.macd):
            self.position.close()
            self.sell()


class MacdDerivCross(TrailingStrategy):

    macd = None
    macd_deriv = None
    singal = None
    threshold = 0.

    def init(self):
        super().init()

        # Precompute macd and signal
        self.macd = self.I(macd, self.data.Close, name="MACD")
        self.macd_deriv, self.signal = self.I(macd_deriv_signal, self.macd, name="MACD'")
        self.set_trailing_sl(2)

    def next(self):
        super().next()

        if crossover(self.macd_deriv, self.signal):
            self.position.close()
            self.buy()

        elif crossover(self.signal, self.macd_deriv):
            self.position.close()
            self.sell()


class MacdGradCross(TrailingStrategy):

    macd = None
    macd_grad = None
    singal = None
    threshold = 0.

    def init(self):
        super().init()

        # Precompute macd and signal
        self.macd = self.I(macd, self.data.Close, name="MACD")
        self.macd_grad, self.signal = self.I(macd_grad_signal, self.macd, name="MACD'")
        self.set_trailing_sl(2)

    def next(self):
        super().next()

        if crossover(self.macd_grad, self.signal):
            self.position.close()
            self.buy()
        
        elif crossover(self.signal, self.macd_grad):
            self.position.close()
            self.sell()


class MacdGradDerivCross(TrailingStrategy):

    macd = None
    macd_grad = None
    macd_deriv = None
    signal = None
    threshold = 0.
    smooth = 0

    def init(self):
        super().init()

        # Precompute macd and signal
        self.macd = self.I(macd, self.data.Close, name="MACD")
        self.macd_grad, self.macd_deriv = self.I(macd_grad_deriv, self.macd, name="MACD'", smooth=self.smooth)
        self.signal = np.zeros(self.macd.shape) + self. threshold
        self.set_trailing_sl(2)

    def next(self):
        super().next()

        if (crossover(self.macd_grad, self.signal) and self.macd_deriv >= 0 or
            crossover(self.macd_deriv, self.signal) and self.macd_grad >= 0):
            self.position.close()
            self.buy()

        elif (crossover(self.signal, self.macd_grad) and self.macd_deriv <= 0 or
            crossover(self.signal, self.macd_deriv) and self.macd_grad <= 0):
            self.position.close()
            self.sell()


class MacdGradCheat(TrailingStrategy):
    """
    Cheating MACD gradient strategy, uses information ahead of time to calculate gradient 
    """
    macd = None
    macd_grad = None
    singal = None
    threshold = 0.

    def init(self):
        super().init()

        # Precompute macd and signal
        self.macd = self.I(macd, self.data.Close, name="MACD")
        self.macd_grad = self.I(pd.Series, np.gradient(self.macd), name="MACD'")
        self.signal = self.I(pd.Series, np.zeros(self.macd.shape), name="Zero")
        self.set_trailing_sl(2)

    def next(self):
        super().next()

        if crossover(self.macd_grad, self.signal):
            self.position.close()
            self.buy()

        elif crossover(self.signal, self.macd_grad):
            self.position.close()
            self.sell()


def simple_moving_avg(values, n):
    """
    Return simple moving average of values, at
    each step taking into account n previous values.
    """
    return pd.Series(values).rolling(n).mean()


def macd_signal(values, smooth: int = 9, **kwargs):

    md = macd(values, **kwargs)
    signal = md.ewm(span=smooth, adjust=False).mean()

    return md, signal


def macd_deriv_signal(values, threshold: float = 0., **kwargs):

    md = macd_deriv(values, **kwargs)
    signal = np.zeros(md.shape) + threshold
    return md, signal


def macd_grad_deriv(values, threshold: float = 0., smooth: int = 0, **kwargs):

    md_grad = macd_grad(values, **kwargs)
    # Have to shift since gradient uses next value in calculation
    md_grad = md_grad.shift(1)

    md_deriv = macd_deriv(values, **kwargs)

    if smooth:
        md_deriv = md_deriv.ewm(span=smooth, adjust=False).mean()

    return md_grad, md_deriv


def macd_grad_signal(values, threshold: float = 0., **kwargs):

    md = macd_grad(values, **kwargs)
    # Have to shift since gradient uses next value in calculation
    md = md.shift(1)

    signal = np.zeros(md.shape) + threshold
    return md, signal


def macd(values, n_fast: int = 12, n_slow: int = 26):
    """
    Return MACD (Moving Average Convergence/Divergence)
    using fast and slow exponential moving averages.
    """
    emaslow = pd.Series(values).ewm(span=n_slow, adjust=False).mean()
    emafast = pd.Series(values).ewm(span=n_fast, adjust=False).mean()
    macd = emafast - emaslow

    return macd


def smoother(func, _smooth: int = 9, *args, **kwargs):

    return func(*args, **kwargs).ewm(span=_smooth, adjust=False).mean()


def macd_deriv(values, **kwargs):

    md = macd(values, **kwargs)
    return pd.Series(md).diff()


def macd_grad(values, **kwargs):

    md = macd(values, **kwargs)
    return pd.Series(np.gradient(md))


def threshold_signal(shape, threshold: float):

    return np.zeros(shape) + threshold


if __name__ == "__main__":

    bt = Backtest(GOOG, MacdSignalCross, cash=10_000, commission=.002)
    stats = bt.run()
    print(stats)
    bt.plot()
