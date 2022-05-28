import h5py
from typing import Union, List


def keys_in_h5(file_path) -> list:
    """Return keys in h5 file, only necessary if not using pandas (otherwise use HDFStore.keys())"""
    with h5py.File(file_path, 'r') as f:
        saved_keys = list(f.keys())

    return saved_keys


def create_h5_key(*args: str) -> str:
    """Create a valid multi-level h5 key from input strings"""
    return "/".join(["", *args])


def h5_key_elements(key: str, index: int = None) -> Union[List, str]:
    """Return h5 key levels from input key
    :param key: HDF5 key, input as string
    :param index: index or level of nested key to return
    """

    elements = key.lstrip("/").split("/")

    if index is None:
        return elements
    else:
        return elements[index]
