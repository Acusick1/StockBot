import unittest
import os
import numpy as np
import pandas as pd
from pandas import HDFStore
from config import settings
from src.api import schemas
from src.api.get_data import DatabaseApi


def test_get_data():

    api = DatabaseApi()
    request = schemas.RequestBase(
        stock="AAPL",
        end_date="2022-08-03",
        period="1y",
        interval=schemas.Interval.from_string("1d")
    )

    api.get_data(request=request)


class TestDatabase(unittest.TestCase):

    api = DatabaseApi()
    key = "/STOCK"
    test_data_file = settings.data_path / "test_data.h5"
    test_df1 = pd.DataFrame(np.random.rand(10, 2))
    test_df2 = pd.DataFrame(np.random.rand(20, 2))

    def setUp(self) -> None:
        if os.path.isfile(self.test_data_file):
            os.remove(self.test_data_file)

        self.file = HDFStore(self.test_data_file)
        self.api.data_file = self.test_data_file

    def test_api(self):

        pass

    def tearDown(self) -> None:
        self.file.close()
        os.remove(self.test_data_file)
