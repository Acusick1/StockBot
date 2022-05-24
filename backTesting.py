import numpy as np
import matplotlib.pyplot as plt
from src.data import get_data
from src.features import features
from strategies.live import LiveStrategy
from src.gen import get_subplot_shape

# TODO: Different data conversions for daily and live (live strat currently using daily pipeleine)


if __name__ == "__main__":

    # input_stocks = ["AAPL", "BTC-USD", "EURUSD=X", "F"]
    input_stocks = ["AAPL", "F", "AMD", "NVDA", "MSFT"]

    dfs = get_data.download_data(input_stocks)
    stocks_data = features.build(dfs)

    subplot_shape = get_subplot_shape(len(input_stocks))

    profit = {}
    fig, ax = plt.subplots(*subplot_shape)
    ax = ax.flatten()
    plt.tight_layout()

    i = 0
    for stock, data in stocks_data.items():
        strategy = LiveStrategy()

        plt.sca(ax[i])
        ax[i].set_title(stock)
        profit[stock] = strategy.main(data)

        i += 1

    print(profit)
