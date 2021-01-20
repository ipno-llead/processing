import re
import pandas as pd
import numpy as np


mdy_date_pattern_1 = re.compile(r"^\d{1,2}/\d{1,2}/\d{2}$")
mdy_date_pattern_2 = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")
year_pattern = re.compile(r"^(19|20)\d{2}$")
year_month_pattern = re.compile(r"^(19|20)\d{4}$")


def clean_date(val):
    """
    Try parsing date with various patterns and return a tuple of (year, month, day)
    """
    if val == "" or pd.isnull(val):
        return "", "", ""
    m = mdy_date_pattern_2.match(val)
    if m is not None:
        [month, day, year] = val.split("/")
        return year, month, day
    m = mdy_date_pattern_1.match(val)
    if m is not None:
        [month, day, year] = val.split("/")
        if year[0] in ["1", "2", "0"]:
            year = "20" + year
        else:
            year = "19" + year
        return year, month, day
    m = year_month_pattern.match(val)
    if m is not None:
        return val[:4], val[4:], ""
    m = year_pattern.match(val)
    if m is not None:
        return val, "", ""
    raise ValueError("unknown date format \"%s\"" % val)


def clean_date_series(series):
    return pd.DataFrame.from_records(series.str.strip().map(clean_date))


def clean_dates(df, cols):
    for col in cols:
        assert col.endswith("_date")
        dates = pd.DataFrame.from_records(df[col].str.strip().map(clean_date))
        prefix = col[:-5]
        dates.columns = [prefix+"_year", prefix+"_month", prefix+"_day"]
        df = pd.concat([df, dates], axis=1)
    df = df.drop(columns=cols)
    return df


def parse_dates_with_known_format(df, cols, format):
    for col in cols:
        assert col.endswith("_date")
        dates = pd.DataFrame.from_records(pd.to_datetime(df[col], format=format).map(lambda x: (
            "", "", "") if pd.isnull(x) else (str(x.year), str(x.month), str(x.day))))
        prefix = col[:-5]
        dates.columns = [prefix+"_year", prefix+"_month", prefix+"_day"]
        df = pd.concat([df, dates], axis=1)
    df = df.drop(columns=cols)
    return df


def clean_salary(series):
    return series.str.strip().str.replace(r"[^\d\.]", "").astype("float64")


def clean_salaries(df, cols):
    for col in cols:
        df.loc[:, col] = clean_salary(df[col])
    return df


def clean_name(series):
    return series.str.strip().str.replace(r"[^\w-]+", " ").str.replace(r"\s+", " ").str.lower().str.strip().fillna("")


def clean_names(df, cols):
    for col in cols:
        df.loc[:, col] = clean_name(df[col])
    return df


def standardize_desc(series):
    return series.str.strip().str.lower()


def standardize_desc_cols(df, cols):
    for col in cols:
        df.loc[:, col] = standardize_desc(df[col])
    return df


def float_to_int_str(df, cols):
    """
    Turn float column to str column
    e.g. [1973.0, np.nan] => ["1973", ""]
    """
    cols_set = set(df.columns)
    for col in cols:
        if col not in cols_set:
            continue
        if df[col].dtype != np.float64:
            continue
        df.loc[:, col] = df[col].fillna(0).astype(
            "int64").astype(str).str.replace(r"^0$", "", regex=True)
    return df
