import deba
from lib.columns import clean_column_names
from lib.clean import clean_names, clean_dates, standardize_desc_cols
from lib.uid import gen_uid
import pandas as pd


def swap_names(df):
    # swap first_name and last_name in first row
    v = df.loc[0, "first_name"]
    df.loc[0, "first_name"] = df.loc[0, "last_name"]
    df.loc[0, "last_name"] = v
    return df


def assign_agency(df):
    df.loc[:, "data_production_year"] = "2020"
    df.loc[:, "agency"] = "madisonville-pd"
    return df


def clean():
    df = pd.read_csv(
        deba.data("raw/madisonville_pd/madisonville_pd_cprr_2010-2020_byhand.csv")
    )
    df = clean_column_names(df)
    df = (
        df.drop(columns=["complaintant"])
        .rename(
            columns={
                "title": "rank_desc",
                "incident_number": "tracking_id",
            }
        )
        .pipe(swap_names)
        .pipe(clean_dates, ["incident_date"])
        .pipe(standardize_desc_cols, ["rank_desc"])
        .pipe(assign_agency)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["agency", "tracking_id"], "allegation_uid")
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_uid")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_madisonville_pd_2010_2020.csv"), index=False)
