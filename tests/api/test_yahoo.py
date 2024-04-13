import time
from config import yahoo_api_settings


def test_make_request(yahoo_api, yahoo_request):
    response = yahoo_api.make_request(
        endpoint=yahoo_request["endpoint"], params=yahoo_request["params"]
    )

    assert response.status_code == 200
    time.sleep(yahoo_api_settings.poll_frequency)


def test_get_stock_history(yahoo_api, single_request):
    response = yahoo_api.get_stock_history(single_request)

    assert response.status_code == 200
    time.sleep(yahoo_api_settings.poll_frequency)
