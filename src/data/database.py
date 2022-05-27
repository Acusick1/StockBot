import pandas as pd


def merge_data(df: pd.DataFrame, h5_file: pd.HDFStore, key: str):

    stored_data = h5_file.get(key) if key in h5_file.keys() else None

    if stored_data is not None:
        df = pd.concat([stored_data, df]).drop_duplicates().sort_index()

    h5_file.put(key, df, format="table")
