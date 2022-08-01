import pandas as pd
import numpy as np


def first_valid_value(sr: pd.Series):
    """Returns first non-na value of series"""
    for _, v in sr.items():
        if not pd.isna(v):
            return v
    return np.NaN
