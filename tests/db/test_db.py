from unittest.mock import patch
from src.db.main import create_fake_data


@patch("yfinance.download")
def test_get_data(yfd, database_api, fake_year_request):

    database_api.get_data(request=fake_year_request)
    assert yfd.call_count == 1


@patch("yfinance.download")
def test_similar_requests(yfd, database_api, fake_year_request, fake_month_request):

    data = create_fake_data(request=fake_year_request)
    yfd.return_value = data

    # Two of the same calls, first should request new data, second should pull existing data
    database_api.get_data(request=fake_year_request)
    database_api.get_data(request=fake_year_request)

    # Shorter request over same period, should also pull existing data
    database_api.get_data(request=fake_month_request)

    assert yfd.call_count == 1

