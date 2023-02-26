import pytest
from config import yahoo_api_settings


def test_single_request(yf_api, single_request):

    data = yf_api.make_request(request=single_request)
    assert data.shape[0] > 0


def test_multi_request(yf_api, multi_request):

    data = yf_api.make_request(request=multi_request)
    assert data.shape[0] > 0


def test_too_many_tickers(yf_api, mocker):
    
    mocker.patch("yfinance.download")

    with pytest.raises(ValueError, match="exceeds"):
        yf_api.request([""] * (yahoo_api_settings.max_stocks_per_request + 1))
    