import numpy as np
import pandas as pd
from backtesting import Backtest, Strategy
from functools import partial
from hyperopt import fmin, hp, space_eval, tpe, STATUS_OK
from sklearn.model_selection import KFold
from typing import Any, Optional
from strategies import daily
from src.db.main import DatabaseApi
from utils import gen
from utils.tickers import get_snp500_tickers
from config import EXAMPLE_STOCKS


def run(data: pd.MultiIndex, strategy: Strategy, params: Optional[dict[str, Any]] = None, **kwargs):

    if params is not None:
        _ = strategy._check_params(strategy, params)

    out, all_bt = {}, {}
    for stock, df in data.groupby(level=0, axis=1):

        df = df.droplevel(0, axis=1)
        df["Close"] = df["Adj Close"]
        df = df.dropna()
        bt = Backtest(df, strategy, cash=1000, commission=.002)

        out[stock] = bt.run(**kwargs)
        all_bt[stock] = bt

    return out, all_bt


def opt(obj_func, param_space: dict[str, Any], **kwargs):

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
    stocks = get_snp500_tickers()[:50]
    data = api.request(stock=stocks, interval="1d", period="1y")

    # strategy = daily.SmaCross

    # param_space = {
    #     "n1": hp.choice("n1", [5, 10, 15]),
    #     "n2": hp.choice("n2", [10, 20, 40]),
    # }

    strategy = daily.MacdGradDerivCross

    param_space = {
        "buy_sell": hp.uniformint("buy_sell", 0, 2),
        "smooth": hp.uniformint("smooth", 0, 12),
        "threshold": hp.uniform("threshold", -0.25, 0.25),
    }

    # strategy = daily.MacdEma

    # param_space = {
    #     "n1": hp.uniformint("n1", 3, 10),
    #     "n2": hp.uniformint("n2", 14, 40),
    #     "threshold1": hp.uniform("threshold1", -1, 1),
    #     "threshold2": hp.uniform("threshold2", -1, 1)
    # }

    kf = KFold(n_splits=5, shuffle=True, random_state=42)
    all_stats, reduced_stats = {}, {}

    i = 0
    for train_id, test_id in kf.split(stocks):

        train_stocks = np.array(stocks)[train_id]
        test_stocks = np.array(stocks)[test_id]

        obj_func = partial(objective, strategy=strategy, data=data[train_stocks])

        best = opt(
            obj_func,
            param_space=param_space,
            max_evals=10
        )

        best_params = space_eval(param_space, best)
        print(best_params)

        stats, all_bt = run(data[test_stocks], strategy=strategy, params=best_params)
        df_stats = pd.DataFrame(stats).transpose()
        
        all_stats[f"run{i}"] = stats

        # Taking mean of numeric columns
        reduced_stats[f"run{i}"] = df_stats.loc[:, ~df_stats.columns.str.startswith("_")].mean(numeric_only=False)

        i += 1

    reduced_stats = pd.concat(reduced_stats, axis=1)
    print(reduced_stats)

    all_stats = gen.multiindex_from_dict(all_stats)
    print(all_stats)
    # for bt in all_bt.values():
    #     bt.plot()