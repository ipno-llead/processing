import re
import pandas as pd
import numpy as np
import datetime


mdy_date_pattern_1 = re.compile(r"^\d{1,2}/\d{1,2}/\d{2}$")
mdy_date_pattern_2 = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")
year_pattern = re.compile(r"^(19|20)\d{2}$")
year_month_pattern = re.compile(r"^(19|20)\d{4}$")
datetime_pattern_1 = re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}\s+\d{1,2}:\d{1,2}$")
month_day_pattern = re.compile(r"^[A-Z][a-z]{2}-\d{1,2}$")


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
    m = month_day_pattern.match(val)
    if m is not None:
        dt = datetime.datetime.strptime(val, "%b-%d")
        return "", str(dt.month).zfill(2), str(dt.day).zfill(2)
    raise ValueError("unknown date format \"%s\"" % val)


def clean_dates(df, cols):
    for col in cols:
        assert col.endswith("_date")
        dates = pd.DataFrame.from_records(df[col].str.strip().map(clean_date))
        prefix = col[:-5]
        dates.columns = [prefix+"_year", prefix+"_month", prefix+"_day"]
        df = pd.concat([df, dates], axis=1)
    df = df.drop(columns=cols)
    return df


def clean_datetime(val):
    if val == "" or pd.isnull(val):
        return "", "", "", "", ""
    m = datetime_pattern_1.match(val)
    if m is not None:
        [date, time] = re.split(r"\s+", val)
        [hour, minute] = time.split(":")
        year, month, day = clean_date(date)
        return year, month, day, "%s:%s" % (hour.zfill(2), minute.zfill(2))
    raise ValueError("unknown datetime format \"%s\"" % val)


def clean_datetimes(df, cols):
    for col in cols:
        assert col.endswith("_datetime")
        dates = pd.DataFrame.from_records(
            df[col].str.strip().map(clean_datetime))
        prefix = col[:-9]
        dates.columns = [prefix+"_year", prefix +
                         "_month", prefix+"_day", prefix+"_time"]
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


def clean_sexes(df, cols):
    for col in cols:
        df.loc[:, col] = df[col].str.strip().str.lower()\
            .str.replace(r"^m$", "male").str.replace(r"^f$", "female")
    return df


def clean_races(df, cols):
    for col in cols:
        df.loc[:, col] = df[col].str.strip().str.lower()\
            .str.replace(r"^w$", "white").str.replace(r"^b$", "black")\
            .str.replace(r"^h$", "hispanic")
    return df


def clean_employment_status(df, cols):
    for col in cols:
        df.loc[:, col] = df[col].str.strip().str.lower()\
            .str.replace(r"^i$", "inactive").str.replace(r"^a$", "active")
    return df


def clean_salary(series):
    return series.str.strip().str.replace(r"[^\d\.]", "").astype("float64")


def clean_salaries(df, cols):
    for col in cols:
        df.loc[:, col] = clean_salary(df[col])
    return df


def clean_name(series):
    return series.str.strip().str.replace(r"[^\w-]+", " ").str.replace(r"\s+", " ")\
        .str.replace(r"\s*-\s*", "-").str.lower().str.strip().fillna("")\
        .str.strip("-")


def clean_names(df, cols):
    for col in cols:
        df.loc[:, col] = clean_name(df[col])
    return df


def standardize_desc(series):
    return series.str.strip().str.lower().fillna("")


def standardize_desc_cols(df, cols):
    for col in cols:
        df.loc[:, col] = standardize_desc(df[col])
    return df


def float_to_int_str(df, cols):
    """
    Turn float values in column into strings without trailing ".0"
    e.g. [1973.0, np.nan, "abc"] => ["1973", "", "abc"]
    """
    cols_set = set(df.columns)
    for col in cols:
        if col not in cols_set:
            continue
        if df[col].dtype == np.float64:
            df.loc[:, col] = df[col].fillna(0).astype(
                "int64").astype(str).str.replace(r"^0$", "", regex=True)
        elif df[col].dtype == np.object:
            idx = df[col].map(lambda v: type(v) == float)
            df.loc[idx, col] = df.loc[idx, col].fillna(0).astype(
                "int64").astype(str).str.replace(r"^0$", "", regex=True)
    return df
