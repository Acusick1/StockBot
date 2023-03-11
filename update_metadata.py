import pandas as pd
from datetime import datetime
from pandas_datareader import data
from time import sleep
from src.db.main import DatabaseApi
from utils.gen import chunk
from config import settings, yahoo_api_settings


def get_stock_metadata(tickers: list[str]) -> pd.DataFrame:

    if len(tickers) > yahoo_api_settings.max_stocks_per_request:
        tickers = chunk(tickers, size=yahoo_api_settings.max_stocks_per_request)

    quote = []
    for group in tickers:
        try:
            print(f"Retreiving metadata for ticker: {group}")
            quote.append(data.get_quote_yahoo(group))
            sleep(yahoo_api_settings.poll_frequency)
        except KeyError as e:
            
            if isinstance(group, str):
                print(f"Error obtaining metadata for ticker: {group} \n{e}")
                return
            else:
                print("Failed. Retrying individual tickers")
                quote.extend([get_stock_metadata([ticker]) for ticker in group])

    if quote:
        quote = pd.concat(quote, axis=0)

    return quote


if __name__ == "__main__":

    db = DatabaseApi()
    print("Updating stock metadata:", datetime.now())
    tickers = db.get_stored_tickers(group="daily")
    
    quote = get_stock_metadata(tickers)
    quote.to_csv(settings.stock_metadata_file)