import unittest
from unittest import mock
from src.data.get_data import FinanceApi, YahooApi

EXAMPLE_STOCKS = ['AAPL', 'F']


class TestFinanceApi(unittest.TestCase):

    def setUp(self) -> None:
        self.api = FinanceApi()

    def test_download_data(self):

        data = self.api.download_data(EXAMPLE_STOCKS)
        self.assertTrue(all(s in data for s in EXAMPLE_STOCKS))

    @mock.patch('yfinance.download', return_value=None)
    @mock.patch('time.sleep', return_value=None)
    def test_download_data_calls(self, sleep, yfd):

        _ = self.api.download_data(EXAMPLE_STOCKS)
        assert yfd.call_count == len(EXAMPLE_STOCKS)
        assert sleep.call_count == len(EXAMPLE_STOCKS) - 1


class TestYahooApi(unittest.TestCase):

    example_request = {
        "endpoint": "https://yfapi.net/v8/finance/spark",
        "params": {
            "symbols": ",".join(EXAMPLE_STOCKS),
            "period": "1mo",
            "interval": "1d"
        }
    }

    def setUp(self) -> None:

        self.api = YahooApi()

    def test_make_request(self):

        response = self.api.make_request(
            endpoint=self.example_request["endpoint"],
            params=self.example_request["params"])

        self.assertEqual(response.status_code, 200)
