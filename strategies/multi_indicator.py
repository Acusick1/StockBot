import numpy as np
import pandas as pd
from backtesting import Backtest
from backtesting.lib import TrailingStrategy
from backtesting.test import GOOG
from hyperopt import hp
from strategies.daily import macd


class Tunable:
    def __init__(self, **params):
        self.params = self.get_default_params()
        self.set_params(**params)

    def set_params(self, **params):
        self.params.update(params)

    def get_params(self, hyper=False):
        if hyper:
            return self.get_param_space() or None
        else:
            return self.params.copy()

    @staticmethod
    def get_default_params():
        return {}

    @staticmethod
    def get_param_space():
        return {}


class Indicator(Tunable):
    def __init__(self, **params):
        super().__init__(**params)

    def __call__(self, data: pd.Series) -> pd.Series:
        pass


class Signal(Tunable):
    def __init__(self, **params):
        super().__init__(**params)

    def __call__(self, data: pd.Series) -> pd.Series:
        pass


class Threshold(Signal):
    def __init__(self, scale: float = 1, value: float = 0.0):
        super().__init__(value=value)
        self.scale = scale

    def __call__(self, data: pd.Series) -> pd.Series:
        return pd.Series(np.zeros(data.shape) + (self.scale * self.params["value"]))

    def get_param_space(self):
        return {"value": hp.uniform("value", -0.05, 0.05)}


class Macd(Indicator):
    def __init__(self, n_fast: int = 12, n_slow: int = 26):
        super().__init__(n_fast=n_fast, n_slow=n_slow)

    def __call__(self, data: pd.Series):
        emaslow = pd.Series(data).ewm(span=self.params["n_slow"], adjust=False).mean()
        emafast = pd.Series(data).ewm(span=self.params["n_fast"], adjust=False).mean()
        macd = emafast - emaslow

        return macd


class MacdSignal(Signal):
    def __init__(self, smooth: int = 9):
        super().__init__(smooth=smooth)

    def __call__(self, macd: pd.Series) -> pd.Series:
        signal = macd.ewm(span=self.params["smooth"], adjust=False).mean()

        return signal


class MacdDeriv(Indicator):
    def __init__(self, n_fast: int = 12, n_slow: int = 26, smooth: int = 9):
        super().__init__(n_fast=n_fast, n_slow=n_slow, smooth=smooth)

    def __call__(self, data: pd.Series) -> pd.Series:
        md = macd(
            data, n_fast=self.params["n_fast"], n_slow=self.params["n_slow"]
        ).diff()

        if self.params["smooth"]:
            md = md.ewm(span=self.params["smooth"]).mean()

        return md


class SignalIndicator:
    def __init__(self, indicator, signal):
        self.indicator = indicator
        self.signal = signal
        self.indicator_param_key = self.indicator.__class__.__name__
        self.signal_param_key = self.signal.__class__.__name__

    def __call__(self, data):
        ind = self.indicator(data)
        signal = self.signal(ind)
        return ind, signal

    def set_params(self, **kwargs):
        if (
            self.indicator_param_key in kwargs
            and kwargs[self.indicator_param_key] is not None
        ):
            self.indicator.set_params(**kwargs[self.indicator_param_key])

        if (
            self.signal_param_key in kwargs
            and kwargs[self.signal_param_key] is not None
        ):
            self.signal.set_params(**kwargs[self.signal_param_key])

    def get_params(self, hyper=False):
        return {
            self.indicator_param_key: self.indicator.get_params(hyper),
            self.signal_param_key: self.signal.get_params(hyper),
        }


class MultiSignalIndicator:
    def __init__(self, signal_indicators: list[SignalIndicator]) -> None:
        self.signal_indicators = signal_indicators
        self.param_keys = [f"si{i}" for i in range(len(signal_indicators))]

    def set_params(self, **params):
        for k, v in zip(self.param_keys, self.signal_indicators):
            if k in params:
                v.set_params(**params[k])

    def get_params(self, hyper=False):
        return {
            k: v.get_params(hyper)
            for k, v in zip(self.param_keys, self.signal_indicators)
        }


class MultiIndicatorStrategy(TrailingStrategy):
    msi: MultiSignalIndicator = None

    def init(self):
        super().init()
        self.prev_signals = np.zeros(len(self.msi.signal_indicators))

        self.indicators = []
        self.signals = []
        for si in self.msi.signal_indicators:
            # Wrapping in an indicator to reveal one at a time and plot
            i, s = self.I(si, self.data.Close)
            self.indicators.append(i)
            self.signals.append(s)

        self.set_trailing_sl(2)

    def next(self):
        super().next()

        signals = self.get_signals()

        # Look for all above, plus one just above (previously below)
        if all(signals > 0) and not all(self.prev_signals > 0):
            self.position.close()
            self.buy()

        elif all(signals < 0) and not all(self.prev_signals < 0):
            self.position.close()
            self.sell()

        self.prev_signals = signals

    def get_signals(self):
        signals = []
        for i, s in zip(self.indicators, self.signals):
            # TODO: Only for threshold signals
            if i > s:
                signals.append(1)
            elif i < -s:
                signals.append(-1)
            else:
                signals.append(0)

        return np.array(signals)


if __name__ == "__main__":
    from pprint import pprint

    indicator1 = Macd()
    signal1 = MacdSignal()

    indicator2 = MacdDeriv()
    signal2 = Threshold()

    si = [
        SignalIndicator(indicator=indicator1, signal=signal1),
        SignalIndicator(indicator=indicator2, signal=signal2),
    ]

    params = si.get_params()
    param_space = si.get_params(hyper=True)

    pprint(params)
    pprint(param_space)

    si = MultiSignalIndicator(signal_indicators=si)

    bt = Backtest(GOOG, MultiIndicatorStrategy, cash=10_000, commission=0.002)
    stats = bt.run(msi=si)
