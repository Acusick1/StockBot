import numpy as np
import pandas as pd
from backtesting import Backtest
from functools import partial
from hyperopt import fmin, hp, space_eval, tpe, STATUS_OK
from typing import Any, Optional
from src.db.main import DatabaseApi
from strategies.multi_indicator import MultiIndicatorStrategy, SignalIndicator, MultiSignalIndicator, Macd, MacdDeriv, MacdSignal, Threshold
from utils.gen import flatten_dict, unflatten_dict
from utils.tickers import get_snp500_tickers
from config import EXAMPLE_STOCKS


def run(data: pd.MultiIndex, msi: MultiSignalIndicator, params: Optional[dict[str, Any]] = None, **kwargs):

    if params is not None:
        msi.set_params(**params)

    out, all_bt = {}, {}
    for stock, df in data.groupby(level=0, axis=1):

        df = df.droplevel(0, axis=1)
        df["Close"] = df["Adj Close"]
        df = df.dropna()
        bt = Backtest(df, MultiIndicatorStrategy, cash=1000, commission=.002)

        out[stock] = bt.run(msi=msi, **kwargs)
        all_bt[stock] = bt

    return out, all_bt


def opt(obj_func, param_space: dict[str, Any], **kwargs):

    return fmin(obj_func, param_space, algo=tpe.suggest, **kwargs)


def multi_indicator_objective(params, msi: MultiIndicatorStrategy, data):

    params = unflatten_dict(params)
    msi.set_params(**params)
    
    stats, _ = run(data=data, msi=msi)
    stats = pd.DataFrame(stats).transpose()

    loss = -stats.loc[stats["# Trades"] > 0, "Sharpe Ratio"].mean()

    return {"loss": loss, "status": STATUS_OK}


if __name__ == "__main__":

    api = DatabaseApi()
    # stocks = EXAMPLE_STOCKS
    stocks = get_snp500_tickers()[:20]
    data = api.request(stock=stocks, interval="1d", period="1y")

    indicator1 = MacdDeriv()
    signal1 = Threshold()

    indicator2 = MacdDeriv()
    signal2 = Threshold()

    si = [
        SignalIndicator(indicator=indicator1, signal=signal1),
        SignalIndicator(indicator=indicator2, signal=signal2)
    ]

    msi = MultiSignalIndicator(signal_indicators=si)
    # param_space = flatten_dict(msi.get_params(hyper=True))

    param_space = {
        "si0__MacdDeriv__smooth": hp.uniformint("short", 0, 7),
        "si1__MacdDeriv__smooth": hp.uniformint("long", 7, 30)
    }

    obj_func = partial(multi_indicator_objective, msi=msi, data=data)

    best = opt(
        obj_func,
        param_space=param_space, 
        max_evals=100
    )

    best_params = space_eval(param_space, best)
    print(best_params)

    stats, all_bt = run(data, msi=msi, params=best_params)

    all_stats = pd.DataFrame(stats).transpose()

    print(all_stats.loc[:, ~all_stats.columns.str.startswith("_")].mean(numeric_only=False))

    for bt in all_bt.values():
        bt.plot()