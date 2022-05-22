import numpy as np
import matplotlib.pyplot as plt
from src.gen import pct_change


class LiveStrategy:

    lookback_period = 5
    # Change required to enter, profit and loss stops, defined by percentages
    entry = 0.002  # Percentage increase required to buy
    profit_stop = 0.001
    loss_stop = -0.0015
    investment = 10

    opened_at = None
    profit = 0

    result = []

    def main(self, data, verbosity=2):
        for i, window in enumerate(data.rolling(window=self.lookback_period)):
            data[i, 'Transaction'] = self.strategy(window['Adj Close'])
            if verbosity == 2 and i > 1:
                self.plot(data.iloc[i-1:i])
                plt.pause(0.05)

        return self.profit

    def strategy(self, price_list):

        res = ""

        if len(price_list) >= self.lookback_period:

            lookback_prices = price_list.to_numpy()
            change = pct_change(lookback_prices)

            # TODO: cumprod(pct_change + 1) - 1 used here for some reason?
            if self.opened_at is None:

                if sum(change) > self.entry:

                    self.opened_at = price_list[-1]
                    res = "buy"
            else:
                prices = np.array([self.opened_at, price_list[-1]])
                change_since_buy = pct_change(prices)

                if change_since_buy[0] > self.profit_stop or change_since_buy[0] < self.loss_stop:

                    self.profit += change_since_buy * self.investment
                    self.opened_at = None
                    res = "sell"
                else:
                    res = "hold"

        return res

    def plot(self, data):

        plt.gcf()
        plt.plot(data['Adj Close'], c='b')

        buy = data.loc[data['Transaction'] == "buy"]
        sell = data.loc[data['Transaction'] == "sell"]

        plt.scatter(buy.index, buy, marker='^', c='g')
        plt.scatter(sell.index, sell, marker='^', c='k')
