from datetime import datetime, timezone

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pandas.tseries.offsets import BDay

from src.db.main import DatabaseApi


class StopLoss:
    def __init__(self) -> None:
        pass

    def calculate():
        raise NotImplementedError()


class AtrStopLoss(StopLoss):
    def __init__(self, multiplier: float = 2.0, period: int = 14) -> None:
        super().__init__()
        self.multiplier = multiplier
        self.period = period

    def calculate(self, df: pd.DataFrame, bought_on: datetime | None = None):
        if bought_on is None:
            bought_on = df.index[0]

        data = get_atr(df.copy(), period=self.period)
        atr_close = data["Close"] - (self.multiplier * data["ATR"])

        return atr_close.loc[bought_on:].expanding(1).max()


class LowStopLoss(StopLoss):
    def __init__(self, period: int = 14) -> None:
        super().__init__()
        self.period = period

    def calculate(self, df: pd.DataFrame, bought_on: datetime | None = None):
        if bought_on is None:
            bought_on = df.index[0]

        stop_loss = df["Low"].rolling(window=self.period).min()
        return stop_loss.loc[bought_on:].expanding(1).max()


def get_true_range(data: pd.DataFrame):
    # Calculate the true range
    return data.assign(
        TR=np.abs(
            np.array(
                [
                    data["High"] - data["Low"],
                    data["High"] - data["Close"],
                    data["Close"] - data["Low"],
                ]
            )
        ).max(axis=0)
    )


def get_atr(data: pd.DataFrame, period: int = 14):
    # Calculate the ATR (average true range)
    data = get_true_range(data)
    return data.assign(ATR=data["TR"].rolling(window=period).mean())


stop_losses = {
    "low": LowStopLoss(),
    "atr conservative": AtrStopLoss(multiplier=1),
    "atr normal": AtrStopLoss(multiplier=2),
}


if __name__ == "__main__":
    db = DatabaseApi()
    data = db.request(stock=["AMZN"], flat=True)

    bought_at = None
    bought_on = db.calendar.tz.localize(datetime(2022, 6, 6, tzinfo=timezone.utc))

    if bought_at is None:
        bought_at = data.loc[bought_on, ["High", "Low"]].mean()

    is_valid = data.loc[bought_on, "Low"] <= bought_at <= data.loc[bought_on, "High"]

    if not is_valid:
        raise ValueError("Not a valid buy price on given date")

    stops = pd.DataFrame(
        {key: loss.calculate(data, bought_on=bought_on) for key, loss in stop_losses.items()},
        index=data.index,
    )

    ax = stops.plot(y=stops.columns, use_index=True)
    ax.plot(data["Close"], label="Close")
    ax.plot(bought_on, bought_at, "go")

    reached = stops.ge(data[["Close"]].values).expanding(1).max().sum(axis=1)

    reached_threshold = 2
    if reached.max() >= reached_threshold:
        sold_on = reached.eq(reached_threshold).idxmax() + BDay(1)
        sold_at = data.loc[sold_on, "Open"]
        ax.plot(sold_on, sold_at, "ro")

    plt.show()
