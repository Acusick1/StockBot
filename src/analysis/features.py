import numpy as np
import pandas as pd
from src.db.main import DatabaseApi
from config import settings


def add_analysis(data: pd.DataFrame):
    # TODO: Vectorised way of doing apply(map) functions
    # TODO: Look into ewm(), MACD, calculate span/windows instead of hardcode
    # Have to use lambda functions when using columns assigned in same call
    data = data.assign(
        MACD=data["Adj Close"].rolling(window=200).mean(),
        Change=lambda df: df["Adj Close"].pct_change(),
        UpMove=lambda df: df["Change"].apply(lambda x: x if x > 0 else 0),
        DownMove=lambda df: df["Change"].apply(lambda x: abs(x) if x < 0 else 0),
        UpAvg=lambda df: df["UpMove"].ewm(span=19).mean(),
        DownAvg=lambda df: df["DownMove"].ewm(span=19).mean(),
    )

    data = data.dropna(how="all")

    data = data.assign(
        RS=data["UpAvg"] / data["DownAvg"],
        RSI=lambda df: df["RS"].apply(lambda x: 100 - (100 / (x + 1))),
    )

    # Initialising transaction column
    data["Transaction"] = ""

    return data


def get_historical_features(data: pd.DataFrame):
    # market = get_market_history()
    # market = market.dropna()
    data = data.dropna()

    adj_close = data.xs("Adj Close", axis=1, level=1)
    high = data.xs("High", axis=1, level=1)
    low = data.xs("Low", axis=1, level=1)

    mean_volume = data.xs("Volume", axis=1, level=1).mean()
    volatility = adj_close.pct_change().std()
    avg_range = high.mean() - low.mean()
    # avg_return = adj_close.pct_change().mean()
    # stock_change = adj_close.pct_change()

    # market_change = market.xs("Adj Close", axis=1, level=1).pct_change()
    # cov = stock_change.cov(market_change)
    # market_variance = market_change.std() ** 2
    # beta = cov / market_variance

    features = pd.DataFrame(
        {
            "volatility": volatility,
            "avg_volume": mean_volume,
            "avg_price_range": avg_range,
            # 'beta': beta,
            # 'avg_return': avg_return,
            # 'momentum': ...,
        }
    )

    return features


def get_market_history(ticker: str = "^GSPC", period="1y"):
    db = DatabaseApi()
    market_history = db.request([ticker], period=period)
    return market_history


def get_stock_metadata(stocks: list[str]):
    metadata = pd.read_csv(settings.data_path / "stock_metadata.csv", index_col=0)
    features = metadata.loc[metadata.index.intersection(stocks), :]

    return features


def calculate_volatility(data: pd.Series):
    """
    Calculate volatility of input data
    """

    # Calculate the daily returns of the stock
    pct_change = data.pct_change()

    # Calculate the volatility (standard deviation) of the daily returns
    volatility = np.sqrt(data.shape[0]) * pct_change.std()
    return volatility


def calculate_weighted_volatility(data: pd.Series, window: int, lambda_: float = 0.95):
    """
    Calculate the volatility using an Exponentially Weighted Moving Average (EWMA) approach.

    Parameters:
    data: A list or pandas.Series of historical prices or returns.
    window: The size of the rolling window.
    lambda_: The weighting factor (smoothing factor).

    Returns:
    float: The EWMA volatility.
    """
    returns = data.pct_change().dropna()
    volatility = returns.ewm(alpha=lambda_, min_periods=window).std()[-1]
    return volatility


def calculate_beta(stock: str, period: str = "1y", comp_stock: str = "^GSPC"):
    """
    Calculate volatility of a stock compared to the market over a given period
    """
    db = DatabaseApi()

    stock_change = db.request([stock], period=period)["Adj Close"].pct_change()
    market_change = db.request([comp_stock], period=period)["Adj Close"].pct_change()

    cov = stock_change.cov(market_change)
    market_variance = market_change.std() ** 2
    beta = cov / market_variance
    return beta


if __name__ == "__main__":
    from ta import add_all_ta_features
    from ta.utils import dropna

    ticker = "MSFT"

    db = DatabaseApi()
    df = db.request(ticker).loc[:, ticker].copy()
    df = dropna(df)

    df = add_all_ta_features(
        df, open="Open", high="High", low="Low", close="Close", volume="Volume"
    )
    print(df)
