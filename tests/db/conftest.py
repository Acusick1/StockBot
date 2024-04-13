import pytest

from config import settings
from src.db import schemas
from src.db.main import DatabaseApi


@pytest.fixture()
def store_file():
    test_store_file = settings.data_path / "test_data.h5"
    test_store_file.unlink(missing_ok=True)
    test_store_file.touch()

    yield test_store_file
    test_store_file.unlink()


@pytest.fixture()
def database_api(store_file):
    db = DatabaseApi(store_path=store_file)
    db.data_file = store_file
    return db


@pytest.fixture(scope="session")
def fake_year_request():
    return schemas.RequestBase(stock="FAKE", end_date="2022-08-03", period="1y", interval="1d")


@pytest.fixture(scope="session")
def fake_month_request():
    return schemas.RequestBase(stock="FAKE", end_date="2022-08-03", period="1mo", interval="1d")
