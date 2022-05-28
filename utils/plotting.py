import numpy as np


def get_subplot_shape(num: int, max_columns: int = 8) -> (int, int):

    columns = min(round(np.sqrt(num)), max_columns)
    quotient, rem = divmod(num, columns)
    rows = quotient + 1 if rem else quotient
    return rows, columns
