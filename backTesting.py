from src.data.get_data import download_data
from src.features.build_features import add_analysis
from strategies.live import LiveStrategy

# TODO: Different data conversions for daily and live (live strat currently using daily pipeleine)


if __name__ == "__main__":

    # input_stocks = ["AAPL", "BTC-USD", "EURUSD=X", "F"]
    input_stocks = ["AAPL", "F"]

    # TODO: Input raw data to pipeline, do swap within
    # dfs = get_historical_data(input_stocks)
    dfs = download_data(input_stocks)
    dfs = dfs.swaplevel(0, 1, axis=1)

    profit = []
    for stock in input_stocks:
        data = add_analysis(dfs[stock])
        strategy = LiveStrategy()
        profit.extend(strategy.main(data['Adj Close']))
