import pytest
from src.db import schemas
from src.db.main import DatabaseApi
from config import settings


@pytest.fixture(scope="function")
def test_file():

    test_data_file = settings.data_path / "test_data.h5"
    test_data_file.unlink(missing_ok=True)
    test_data_file.touch()

    yield test_data_file
    test_data_file.unlink()


@pytest.fixture(scope="function")
def database_api(test_file):

    db = DatabaseApi()
    db.data_file = test_file
    yield db


@pytest.fixture(scope="session")
def fake_year_request():
    request = schemas.RequestBase(
        stock="FAKE",
        end_date="2022-08-03",
        period="1y",
        interval="1d"
    )

    yield request


@pytest.fixture(scope="session")
def fake_month_request():
    request = schemas.RequestBase(
        stock="FAKE",
        end_date="2022-08-03",
        period="1mo",
        interval="1d"
    )

    yield request
