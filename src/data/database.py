import pandas as pd
from src.settings import VALID_INTERVALS
from src.gen import keys_in_hdf
from pathlib import Path
from typing import Union


def get_interval_filename(interval: VALID_INTERVALS) -> str:

    return f"inter{interval}_data.hdf5"


def merge_data(df: pd.DataFrame, file_path: Union[Path, str], key: str):

    stored_data = pd.read_hdf(file_path, key=key) if file_path.exists() and key in keys_in_hdf(file_path) else None

    if stored_data is not None:
        df = pd.concat([stored_data, df]).drop_duplicates()

    df.to_hdf(str(file_path), key=key, mode='a')
