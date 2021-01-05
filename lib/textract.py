import pandas as pd
import numpy as np


def realign_series_ws(series):
    values = series.dropna().str.strip().str.split(" ").to_list()
    return pd.Series([item for sublist in values for item in sublist if item])


def realign_series_val(series, to_split):
    values = series.dropna().str.strip().to_list()
    to_split = set(to_split)
    flat_list = []
    for v in values:
        if v in to_split:
            for sv in v.split(" "):
                flat_list.append(sv)
        else:
            flat_list.append(v)
    return pd.Series([item for item in flat_list if item])


def parse_textract_datetime(series):
    date_comp = series.fillna("").str.split("/")
    date_comp = date_comp.map(lambda x: [""] if len(x) <= 1 else [
        x[0].rjust(2, "0"),
        x[1].rjust(2, "0"),
        x[2] if len(x[2]) == 4 else (
            "20"+x[2] if int(x[2][0]) <= 2 else "19"+x[2]),
    ])
    return pd.to_datetime(date_comp.str.join("/")
                          .where(lambda x: x != "", np.NaN), format="%m/%d/%Y")
