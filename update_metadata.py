from datetime import datetime, timezone
from time import sleep

import pandas as pd
from pandas_datareader import data

from config import settings, yahoo_api_settings
from src.db.main import DatabaseApi
from utils.gen import batch


@batch(size=yahoo_api_settings.max_stocks_per_request, concat_axis=0)
def get_stock_metadata(tickers: list[str]) -> pd.DataFrame:
    try:
        print(f"Retreiving metadata for ticker: {tickers}")
        metadata = data.get_quote_yahoo(tickers)
        sleep(yahoo_api_settings.poll_frequency)
        return metadata

    except KeyError as e:
        print(f"Failed: {repr(e)}")
        return None


if __name__ == "__main__":
    db = DatabaseApi()
    print("Updating stock metadata:", datetime.now(tz=timezone.utc))
    tickers = db.get_stored_tickers(group="daily")

    metadata = get_stock_metadata(tickers)
    metadata.to_csv(settings.stock_metadata_file)
