import re
import pandas as pd

m_d_y_date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$')


def to_datetime_series(series):
    if series.dtype.name == "datetime64[ns]":
        return series
    elif series.dtype.name == "object" and type(series[0]) == str:
        if m_d_y_date_pattern.match(series[0]) is not None:
            return pd.to_datetime(series, format='%m/%d/%Y', errors='raise')
        raise Exception("unknown format: %s" % series[0])
    raise Exception("to_datetime can't deal with series: %s", series)


def combine_date_columns(df, year_col, month_col, day_col):
    dates = df[[year_col, month_col, day_col]]
    dates.columns = ["year", "month", "day"]
    return pd.to_datetime(dates)
