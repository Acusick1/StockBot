import numpy as np
import pandas as pd
from typing import Dict
from src.db.main import DatabaseApi


def add_analysis(data: pd.DataFrame):

    # TODO: Vectorised way of doing apply(map) functions
    # TODO: Look into ewm(), MACD, calculate span/windows instead of hardcode
    # Have to use lambda functions when using columns assigned in same call
    data = data.assign(MACD=data['Adj Close'].rolling(window=200).mean(),
                       Change=lambda df: df['Adj Close'].pct_change(),
                       UpMove=lambda df: df['Change'].apply(lambda x: x if x > 0 else 0),
                       DownMove=lambda df: df['Change'].apply(lambda x: abs(x) if x < 0 else 0),
                       UpAvg=lambda df: df['UpMove'].ewm(span=19).mean(),
                       DownAvg=lambda df: df['DownMove'].ewm(span=19).mean(),
                       )

    data = data.dropna(how='all')

    data = data.assign(RS=data['UpAvg']/data['DownAvg'],
                       RSI=lambda df: df['RS'].apply(lambda x: 100 - (100/(x + 1)))
                       )

    # Initialising transaction column
    data['Transaction'] = ""

    return data


def format_data(data: pd.DataFrame):

    # data = data.swaplevel(0, 1, axis=1)

    return data


def build(data: Dict[str, pd.DataFrame] = None):
    """Build features function to pipeline raw data to usable data containing features etc. within models"""

    # TODO: Better way to do this, preferably without reassigning data, ideally doing so in a vectorised fashion
    # TODO: List comprehension once confident in implementation and tests built
    for stock, df in data.items():
        df = format_data(df)
        data[stock] = add_analysis(df)

    return data


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
    Calculate volatility of a stock compared to the market over a give period
    """
    api = DatabaseApi()
    
    stock_change = api.request([stock], period=period)["Adj Close"].pct_change()
    market_change = api.request([comp_stock], period=period)["Adj Close"].pct_change()

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
    
    df = add_all_ta_features(df, open="Open", high="High", low="Low", close="Close", volume="Volume")
    print(df)