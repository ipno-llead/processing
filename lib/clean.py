import datetime
import pandas as pd


def clean_date(series):
    return series.str.split("/").map(
        lambda x: datetime.date(int(x[2]), int(x[0]), int(x[1]))
        if isinstance(x, list) else pd.NaT)


def parse_dates(df, cols, format):
    for col in cols:
        df.loc[:, col] = pd.to_datetime(df[col], format=format)
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
