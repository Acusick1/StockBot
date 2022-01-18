import numpy as np
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

    def main(self, data):
        for window in data.rolling(window=self.lookback_period):
            self.strategy(window)

        return self.profit

    def strategy(self, price_list):

        if len(price_list) >= self.lookback_period:

            lookback_prices = price_list.to_numpy()
            change = pct_change(lookback_prices)

            # TODO: cumprod(pct_change + 1) - 1 used here for some reason?
            if self.opened_at is None:

                if sum(change) > self.entry:

                    self.opened_at = price_list[-1]
            else:
                prices = np.array([self.opened_at, price_list[-1]])
                change_since_buy = pct_change(prices)

                if change_since_buy[0] > self.profit_stop or change_since_buy[0] < self.loss_stop:

                    self.profit += change_since_buy * self.investment
                    self.opened_at = None
