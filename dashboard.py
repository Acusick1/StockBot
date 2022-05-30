import numpy as np
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sbn
from matplotlib import ticker
from typing import Union, Dict, List, Tuple
from src.settings import EXAMPLE_STOCKS, VALID_PERIODS, VALID_INTERVALS, TIME_MAPPINGS
from src.data.get_data import DatabaseApi
from src.data.apis import FinanceApi
from utils.plotting import get_subplot_shape

api = FinanceApi()


# @st.cache
def load_data(tickers: Union[List, Tuple], database: DatabaseApi):

    return database.get_data(tickers)


def plot_stock(data: Dict):

    def format_date(x, pos=None):
        thisind = np.clip(int(x + 0.5), 0, nums - 1)
        return df.index[thisind].strftime('%Y-%m-%d %H:%M')

    subplot_shape = get_subplot_shape(len(data.keys()))

    fig, ax = plt.subplots(*subplot_shape)
    if any(subplot_shape > 1):
        ax = ax.flatten()
        plt.tight_layout()
    else:
        ax = [ax]

    i = 0
    for stock, df in data.items():

        nums = np.arange(df.shape[0])
        plt.sca(ax[i])
        ax[i].set_title(stock)
        sbn.lineplot(x=nums, y=df["Adj Close"])
        # ax[i].xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
        i += 1

    # fig.autofmt_xdate()
    st.pyplot(fig)


def main():
    st.multiselect("Ticker",
                   options=EXAMPLE_STOCKS,
                   key="ticker_picker")

    st.select_slider("Period",
                     options=VALID_PERIODS,
                     key="period_picker")

    st.select_slider("Interval",
                     options=VALID_INTERVALS,
                     value=VALID_INTERVALS[0],
                     key="interval_picker")

    tickers = st.session_state.ticker_picker
    period = st.session_state.period_picker
    interval = st.session_state.interval_picker

    try:
        database = DatabaseApi(api=api, period=TIME_MAPPINGS[period], interval=TIME_MAPPINGS[interval])
        if tickers:
            df = load_data(tickers, database=database)
            plot_stock(df)

    except AssertionError:
        st.write("Interval must be less than period")


if __name__ == "__main__":

    main()
