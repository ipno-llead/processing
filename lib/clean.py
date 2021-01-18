import datetime
import pandas as pd


def clean_date(series):
    return series.str.split("/").map(
        lambda x: datetime.date(int(x[2]), int(x[0]), int(x[1]))
        if isinstance(x, list) else pd.NaT)


def clean_salary(series):
    return series.str.strip().str.replace("$", "", regex=False).astype("float64")


def clean_name(series):
    return series.str.strip().str.replace(r"[^\w-]+", " ").str.replace(r"\s+", " ").str.lower().str.strip().fillna("")


def clean_rank(series):
    return series.str.strip().str.lower()
