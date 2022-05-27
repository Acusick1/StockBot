import unittest
import os
import numpy as np
import pandas as pd
from pandas import HDFStore
from src.settings import DATA_PATH
from src.data import database


class TestDatabase(unittest.TestCase):

    test_data_file = DATA_PATH / "test_data.h5"
    test_df1 = pd.DataFrame(np.random.rand(10, 2))
    test_df2 = pd.DataFrame(np.random.rand(20, 2))

    def setUp(self) -> None:
        if os.path.isfile(self.test_data_file):
            os.remove(self.test_data_file)

        self.file = HDFStore(self.test_data_file)

    def test_merge_data(self):
        key = "/test"
        self.file.put(key, self.test_df1, format="table")
        database.merge_data(self.test_df1, h5_file=self.file, key=key)

        self.assertEqual(
            pd.DataFrame(self.file.get(key)).shape[0], self.test_df1.shape[0]
        )

        database.merge_data(self.test_df2, h5_file=self.file, key=key)

        self.assertEqual(
            pd.DataFrame(self.file.get(key)).shape[0], self.test_df1.shape[0] + self.test_df2.shape[0]
        )

    def tearDown(self) -> None:
        self.file.close()
        os.remove(self.test_data_file)
