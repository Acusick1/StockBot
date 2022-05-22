import matplotlib.pyplot as plt
from src.data import get_data
from src.features import build_features
from strategies.live import LiveStrategy

# TODO: Different data conversions for daily and live (live strat currently using daily pipeleine)


if __name__ == "__main__":

    # input_stocks = ["AAPL", "BTC-USD", "EURUSD=X", "F"]
    input_stocks = ["AAPL", "F"]

    # dfs = get_historical_data(input_stocks)
    dfs = get_data.download_fixed_data(input_stocks)
    stocks_data = build_features.main(dfs)

    profit = {}
    for stock, data in stocks_data.items():
        strategy = LiveStrategy()
        profit[stock] = strategy.main(data)

        """
        buy = data['Adj Close'].loc[data['result'] == "buy"]
        sell = data['Adj Close'].loc[data['result'] == "sell"]
        plt.figure()
        plt.scatter(buy.index, buy, marker='^', c='g')
        plt.scatter(sell.index, sell, marker='^', c='k')
        plt.plot(data['Adj Close'])
        plt.show()
        """
    print(profit)
