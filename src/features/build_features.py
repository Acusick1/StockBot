import pandas as pd


def add_analysis(data: pd.DataFrame):

    # TODO: Vectorised way of doing applymap functions
    # TODO: Look into ewm(), calculate span instead of hardcode
    # Have to use lambda functions when using columns assigned in same call
    data = data.assign(MACD=data['Adj Close'].rolling(window=200).mean(),
                       Change=lambda df: df['Adj Close'].pct_change(),
                       UpMove=lambda df: df['Change'].apply(lambda x: x if x > 0 else 0),
                       DownMove=lambda df: df['Change'].apply(lambda x: abs(x) if x < 0 else 0),
                       UpAvg=lambda df: df['UpMove'].ewm(span=19).mean(),
                       DownAvg=lambda df: df['DownMove'].ewm(span=19).mean(),
                       )

    data = data.dropna()

    data = data.assign(RS=data['UpAvg']/data['DownAvg'],
                       RSI=lambda df: df['RS'].apply(lambda x: 100 - (100/(x + 1)))
                       )

    return data
