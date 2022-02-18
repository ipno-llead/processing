import pandas as pd
import bolo
from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates, standardize_desc_cols, clean_names
from lib.uid import gen_uid


def clean_rank_desc(df):
    df.loc[:, "rank_desc"] = df.rank_desc.str.replace(
        r"^officerr", "officer", regex=True
    )
    return df


def clean():
    df = (
        pd.read_csv(bolo.data("raw/benton_pd/benton_pd_cprr_2015_2021_byhand.csv"))
        .pipe(clean_column_names)
        .pipe(clean_rank_desc)
        .pipe(clean_dates, ["receive_date"])
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(standardize_desc_cols, ["allegation_desc", "disposition"])
        .pipe(set_values, {"agency": "Benton PD"})
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(gen_uid, ["uid", "allegation_desc", "disposition"], "allegation_uid")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(bolo.data("clean/cprr_benton_pd_2015_2021.csv"), index=False)
