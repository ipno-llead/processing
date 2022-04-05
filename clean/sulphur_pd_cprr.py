import pandas as pd
import deba
from lib.clean import clean_dates, standardize_desc_cols, clean_names
from lib.columns import set_values
from lib.uid import gen_uid


def clean_allegation(df):
    df.loc[:, "allegation"] = (
        df.allegation.fillna("")
        .str.replace(r"^excessive force$", "excessive use of force", regex=True)
        .str.replace(r"\/animal cruelty$", " (animal cruelty)", regex=True)
    )
    return df


def clean_disposition(df):
    df.loc[:, "disposition"] = df.disposition.str.replace(r"\,", ";", regex=True)
    return df


def clean():
    df = (
        pd.read_csv(deba.data("raw/sulphur_pd/sulphur_pd_cprr_2014_2019.csv"))
        .rename(columns={"date_received": "receive_date"})
        .pipe(clean_dates, ["receive_date", "disposition_date"])
        .pipe(clean_allegation)
        .pipe(clean_disposition)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(
            standardize_desc_cols,
            ["tracking_id", "complainant", "tracking_id", "rank_desc", "disposition"],
        )
        .pipe(set_values, {"agency": "Sulphur PD"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["uid", "allegation", "disposition", "tracking_id"],
            "allegation_uid",
        )
        .dropna()
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_sulphur_pd_2014_2019.csv"), index=False)
