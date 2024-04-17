import pytest

from src.db import schemas


@pytest.fixture(scope="session")
def fake_year_request():
    return schemas.RequestBase(stock="FAKE", end_date="2022-08-03", period="1y", interval="1d")


@pytest.fixture(scope="session")
def fake_month_request():
    return schemas.RequestBase(stock="FAKE", end_date="2022-08-03", period="1mo", interval="1d")
