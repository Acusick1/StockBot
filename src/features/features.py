import pandas as pd
from typing import Dict


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
