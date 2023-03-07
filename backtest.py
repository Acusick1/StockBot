import numpy as np
import pandas as pd
from backtesting import Backtest, Strategy
from functools import partial
from hyperopt import fmin, hp, space_eval, tpe, STATUS_OK
from typing import Any, Optional
from strategies import daily
from src.db.main import DatabaseApi
from utils.tickers import get_snp500
from config import EXAMPLE_STOCKS


def run(data: pd.MultiIndex, strategy: Strategy, params: Optional[dict[str, Any]] = None):

    if params is not None:
        _ = strategy._check_params(strategy, params)

    out, all_bt = {}, {}
    for stock, df in data.groupby(level=0, axis=1):

        df = df.droplevel(0, axis=1)
        df["Close"] = df["Adj Close"]
        df = df.dropna()
        bt = Backtest(df, strategy, cash=1000, commission=.002)

        out[stock] = bt.run()
        all_bt[stock] = bt

    return out, all_bt


def opt(data: pd.MultiIndex, strategy: Strategy, param_space: dict[str, Any], **kwargs):
    
    obj_func = partial(objective, strategy=strategy, data=data)

    return fmin(obj_func, param_space, algo=tpe.suggest, **kwargs)


def objective(params, strategy, data):

    _ = strategy._check_params(strategy, params)
    stats, _ = run(data=data, strategy=strategy)
    stats = pd.DataFrame(stats).transpose()

    ## Different functions for loss
    # loss = -(stats["Return [%]"] + 100).min()
    # loss = -stats["Return [%]"].sum()
    # loss = -stats["Sortino Ratio"].mean()
    loss = -stats.loc[stats["# Trades"] > 0, "Sharpe Ratio"].mean()
    # loss = -(stats.loc[stats["# Trades"] > 0, "Max. Drawdown [%]"]).mean()

    return {"loss": loss, "status": STATUS_OK}


if __name__ == "__main__":

    api = DatabaseApi()
    # stocks = EXAMPLE_STOCKS
    stocks = get_snp500()[:50]
    data = api.request(stock=stocks, interval="1d", period="1y")

    # strategy = daily.SmaCross

    # strategy_param_space = {
    #     "n1": hp.choice("n1", [5, 10, 15]),
    #     "n2": hp.choice("n2", [10, 20, 40]),
    # }

    strategy = daily.MacdGradDerivCross

    strategy_param_space = {
        "smooth": hp.uniformint("smooth", 0, 12),
        "threshold": hp.uniform("threshold", -0.25, 0.25),
    }

    best = opt(
        data, 
        strategy=strategy,
        param_space=strategy_param_space, 
        max_evals=100
    )

    best_params = space_eval(strategy_param_space, best)
    print(best_params)

    stats, all_bt = run(data, strategy=strategy, params=best_params)

    all_stats = pd.DataFrame(stats).transpose()

    print(all_stats.mean(numeric_only=False))

    # for bt in all_bt.values():
    #     bt.plot()