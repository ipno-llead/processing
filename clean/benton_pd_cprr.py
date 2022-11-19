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
    ).str.replace(r"\n", " ", regex=True)
    return df


def clean_disposition14(df):
    df.loc[:, "disposition"] = df.disposition.fillna("").str.replace(
        r"unsubstantiated", "un-substantiated", regex=False
    )
    return df


def standardize_dates(df):
    df.loc[:, "receive_date"] = df.receive_date.str.replace(r"-", "/", regex=False)
    df.loc[:, "resignation_date"] = df.resignation_date.str.replace(r"-", "/", regex=False)
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
        .pipe(set_values, {"agency": "benton-pd"})
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
        .pipe(clean_allegation_desc)
        .pipe(set_values, {"agency": "benton-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["allegation_desc", "disposition", "uid"], "allegation_uid")
    )
    return df




def clean04():
    df = (
        pd.read_csv(deba.data("raw/benton_pd/benton_pd_cprr_2004_2008.csv"))
        .pipe(standardize_dates)
        .pipe(clean_dates, ["receive_date", "resignation_date"])
        .pipe(standardize_desc_cols, ["allegation_desc", "action", "rank_desc"])
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(set_values, {"agency": "benton-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["allegation_desc", "disposition", "uid"], "allegation_uid")
    )
    return df


if __name__ == "__main__":
    df04 = clean04()
    df21 = clean21()
    df14 = clean14()
    df21.to_csv(deba.data("clean/cprr_benton_pd_2015_2021.csv"), index=False)
    df14.to_csv(deba.data("clean/cprr_benton_pd_2009_2014.csv"), index=False)
    df04.to_csv(deba.data("clean/cprr_benton_pd_2004_2008.csv"), index=False)
