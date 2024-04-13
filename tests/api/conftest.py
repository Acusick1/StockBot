from datetime import datetime, timezone

import pytest
from pandas.tseries.offsets import BDay

from config import EXAMPLE_STOCKS
from src.api.main import FinanceApi, YahooApi
from src.db import schemas

last_bday = datetime.now(tz=timezone.utc) - BDay(1)


@pytest.fixture(scope="session")
def yf_api():
    return FinanceApi()


@pytest.fixture(scope="session")
def yahoo_api():
    return YahooApi()


@pytest.fixture(scope="session")
def yahoo_request():
    return {
        "endpoint": "https://yfapi.net/v8/finance/spark",
        "params": {
            "symbols": ",".join(EXAMPLE_STOCKS),
            "period": "1mo",
            "interval": "1d",
        },
    }


@pytest.fixture(scope="session")
def single_request():
    return schemas.RequestBase(stock="AAPL", period="1y")


@pytest.fixture(scope="session")
def multi_request():
    return schemas.RequestBase(stock=("AAPL", "MSFT"), period="1y")
