import pytest
from datetime import datetime
from pandas.tseries.offsets import BDay
from src.api.main import YahooApi, FinanceApi
from src.db import schemas
from config import EXAMPLE_STOCKS

last_bday = datetime.today() - BDay(1)


@pytest.fixture(scope="session")
def yf_api():

    yield FinanceApi()


@pytest.fixture(scope="session")
def yahoo_api():

    yield YahooApi()


@pytest.fixture(scope="session")
def yahoo_request():

    yield {
        "endpoint": "https://yfapi.net/v8/finance/spark",
        "params": {
            "symbols": ",".join(EXAMPLE_STOCKS),
            "period": "1mo",
            "interval": "1d"
        }
    }


@pytest.fixture(scope="session")
def single_request():

    yield schemas.RequestBase(stock="AAPL", period="1y")


@pytest.fixture(scope="session")
def multi_request():

    return schemas.RequestBase(stock=("AAPL", "MSFT"), period="1y")
