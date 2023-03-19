import numpy as np
import pandas as pd
import wrapt
from collections import abc
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union


def batch(size: int, retry_items: bool = True, concat_axis: Optional[int] = None):
    """
    A decorator to processes a large number of inputs through a function in batches of a set size.

    Parameters
    ----------
    size: The maximum number of items to pass to the function at a time.
    retry_items: Whether to retry items individually if a batch fails.
    concat_axis: Axis to concatenate results, only necessary if output from original function is a Series or DataFrame.

    Returns
    -------
    function: The decorated function.
    """

    @wrapt.decorator
    def wrapper(func, instance, args, kwargs) -> list[Any]:
        
        if args:
            input_list, args = args[0], args[1:]
        else:
            input_list = kwargs.pop(next(iter(kwargs)))

        results = []
        for i in range(0, len(input_list), size):
            
            batch = input_list[i : i + size]
            result = func(batch, *args, **kwargs)
            
            # Retry if no return
            if result is None and retry_items:
                print("No return from batch, retrying items individually")
                results.extend([func(item, *args, **kwargs) for item in batch])
            else:
                results.append(result)

        if isinstance(results[0], (pd.Series, pd.DataFrame)) and concat_axis is not None:
            results = pd.concat(results, axis=concat_axis)

        elif isinstance(results[0], list):
            results = [item for sublist in results for item in sublist]

        return results
    return wrapper


def chunk(data: Iterable, size: int):

    return [data[i : i + size] for i in range(0, len(data), size)]


def validate_strict_args(inp: Any, options: Union[List, Tuple], name: str, optional: bool = False) -> None:
    if not (inp in options or (optional and inp is None)):
        raise ValueError(f"Inputs {name}: {inp} must be one of: {options}")


def pct_change(data: list[float]):
    if len(data) == 1:
        out = 0
    else:
        data = np.array(data)
        out = 100 * (data[1:] - data[:-1]) / data[:-1]

    return out.squeeze()


def dataframe_from_dict(d: dict) -> pd.DataFrame:
    """Creating pandas dataframe from dictionary.
    Wrapper for pd.DataFrame.from_dict for dictionaries with metadata, which will propagate singleton key value pairs
    across full dataframe. Uses dataframe attrs, which is for metadata, but still experimental"""

    df = pd.DataFrame.from_dict(d)

    for key, value in d.items():
        if not isinstance(value, abc.Iterable) or isinstance(value, str):
            df.drop(columns=key, inplace=True)
            df.attrs[key] = value

    return df


def get_key_from_value(d: Dict[str, Any], v: Any):
    return list(d.keys())[list(d.values()).index(v)]


def flatten_dict(nested_dict, sep="__"):
    flattened_dict = {}
    for key, value in nested_dict.items():
        if isinstance(value, dict):
            flattened_subdict = flatten_dict(value, sep=sep)
            for subkey, subvalue in flattened_subdict.items():
                flattened_dict[key + sep + subkey] = subvalue
        else:
            flattened_dict[key] = value
    return flattened_dict


def unflatten_dict(flattened_dict, sep="__"):
    nested_dict = {}
    for key, value in flattened_dict.items():
        keys = key.split(sep)
        subdict = nested_dict
        for k in keys[:-1]:
            if k not in subdict:
                subdict[k] = {}
            subdict = subdict[k]
        subdict[keys[-1]] = value
    return nested_dict
