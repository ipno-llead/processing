import pandas as pd
import deba
from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates, standardize_desc_cols, clean_names
from lib.uid import gen_uid


def clean_rank_desc(df):
    df.loc[:, "rank_desc"] = df.rank_desc.str.replace(
        r"^officerr", "officer", regex=True
    )
    return df


def clean_allegation_desc(df):
    df.loc[:, "allegation_desc"] = df.allegation_desc.str.replace(
        r"\biinformation\b", "information", regex=True
    )
    return df


def clean_disposition14(df):
    df.loc[:, "disposition"] = df.disposition.fillna("").str.replace(
        r"unsubstantiated", "un-substantiated", regex=False
    )
    return df


def clean21():
    df = (
        pd.read_csv(deba.data("raw/benton_pd/benton_pd_cprr_2015_2021_byhand.csv"))
        .pipe(clean_column_names)
        .pipe(clean_rank_desc)
        .pipe(clean_allegation_desc)
        .pipe(clean_dates, ["receive_date"])
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(standardize_desc_cols, ["allegation_desc", "disposition"])
        .pipe(set_values, {"agency": "Benton PD"})
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(gen_uid, ["uid", "allegation_desc", "disposition"], "allegation_uid")
    )
    return df


def clean14():
    df = (
        pd.read_csv(deba.data("raw/benton_pd/benton_pd_cprr_2009_2014_byhand.csv"))
        .pipe(clean_dates, ["receive_date"])
        .pipe(standardize_desc_cols, ["rank_desc"])
        .pipe(clean_disposition14)
        .pipe(standardize_desc_cols, ["allegation_desc"])
        .pipe(set_values, {"agency": "Benton PD"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["allegation_desc", "disposition", "uid"], "allegation_uid")
    )
    return df


if __name__ == "__main__":
    df21 = clean21()
    df14 = clean14()
    df21.to_csv(deba.data("clean/cprr_benton_pd_2015_2021.csv"), index=False)
    df14.to_csv(deba.data("clean/cprr_benton_pd_2009_2014.csv"), index=False)
