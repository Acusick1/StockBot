import matplotlib.pyplot as plt
from strategies.live import LiveStrategy
from src.features import features
from src.data.apis import FinanceApi
from src.gen import get_subplot_shape
from src.data.get_data import get_stock_data

# TODO: Different data conversions for daily and live (live strat currently using daily pipeleine)


if __name__ == "__main__":

    # input_stocks = ["AAPL", "BTC-USD", "EURUSD=X", "F"]
    input_stocks = ["AAPL", "F", "AMD", "NVDA", "MSFT"]

    api_obj = FinanceApi(interval="1d", period="1y")
    dfs = get_stock_data(input_stocks, api=api_obj)
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
