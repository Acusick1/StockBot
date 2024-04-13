import numpy as np
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sbn
from datetime import datetime
from src.api.main import FinanceApi
from src.db.schemas import RequestBase, valid_intervals, valid_periods
from src.db.main import DatabaseApi
from utils.plotting import get_subplot_shape
from config import EXAMPLE_STOCKS

api = FinanceApi()


def plot_stock(data: pd.DataFrame):
    def format_date(x, pos=None):
        thisind = np.clip(int(x + 0.5), 0, nums - 1)
        return df.index[thisind].strftime("%Y-%m-%d %H:%M")

    subplot_shape = get_subplot_shape(len(data.columns.unique(0)))

    fig, ax = plt.subplots(*subplot_shape)
    if any(subplot_shape > 1):
        ax = ax.flatten()
        plt.tight_layout()
    else:
        ax = [ax]

    i = 0
    for stock, df in data.groupby(level=0, axis=1):
        df = df.droplevel(0, axis=1)
        nums = np.arange(df.shape[0])
        plt.sca(ax[i])
        ax[i].set_title(stock)
        sbn.lineplot(x=nums, y=df["Adj Close"])
        # ax[i].xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
        i += 1

    # fig.autofmt_xdate()
    st.pyplot(fig)


def main():
    st.multiselect("Ticker", options=EXAMPLE_STOCKS, key="ticker_picker")

    st.select_slider("Period", options=valid_periods, key="period_picker")

    st.select_slider(
        "Interval",
        options=valid_intervals,
        value=valid_intervals[0],
        key="interval_picker",
    )

    database = DatabaseApi(api=api)

    tickers = st.session_state.ticker_picker
    period = st.session_state.period_picker
    interval = st.session_state.interval_picker

    valid_times = pd.unique([*valid_intervals, *valid_periods])

    if np.where(valid_times == interval) < np.where(valid_times == period):
        if tickers:
            request = RequestBase(
                stock=tickers,
                period=period,
                interval=interval,
                end_date=datetime.today(),
            )
            df = database.get_data(request)

            if df is not None:
                plot_stock(df)

    else:
        st.write("Interval must be less than period")


if __name__ == "__main__":
    main()
