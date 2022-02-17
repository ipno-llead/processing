import dirk
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import standardize_desc_cols, clean_dates
from lib.uid import gen_uid


def clean_allegation(df):
    df.loc[:, "allegation"] = (
        df.allegation.str.lower()
        .str.strip()
        .str.replace(r"\,", ";", regex=True)
        .str.replace("tor ead", "to read", regex=False)
        .str.replace("non caring", "non-caring", regex=False)
    )
    return df


def clean_disposition(df):
    df.loc[:, "disposition"] = df.disposition.str.replace(
        "found to be in ", "", regex=False
    )
    return df


def clean_action(df):
    df.loc[:, "action"] = df.action.str.replace(r"\(pening\)", "- pending", regex=True)
    return df


def clean():
    df = (
        pd.read_csv(dirk.data("raw/maurice_pd/maurice_cprr_2020_2021_byhand.csv"))
        .pipe(clean_column_names)
        .pipe(clean_allegation)
        .pipe(clean_disposition)
        .pipe(clean_action)
        .pipe(standardize_desc_cols, ["first_name", "last_name"])
        .pipe(clean_dates, ["incident_date"])
        .pipe(set_values, {"agency": "Maurice PD"})
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(
            gen_uid,
            [
                "uid",
                "allegation",
                "disposition",
                "action",
                "incident_year",
                "incident_month",
                "incident_day",
            ],
            "allegation_uid",
        )
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(dirk.data("clean/cprr_maurice_pd_2020_2021.csv"), index=False)
