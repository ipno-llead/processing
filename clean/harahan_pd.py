from lib.columns import clean_column_names
import deba
from lib.clean import clean_dates, clean_names
from lib.uid import gen_uid
import pandas as pd


def split_rank_date(df):
    rank_date = (
        df.date_of_rank.str.strip()
        .str.lower()
        .fillna("")
        .str.extract(r"^(\w+)\s+([-\d]+)$")
    )
    df.loc[:, "rank_desc"] = (
        rank_date[0].fillna("").str.replace("sgt", "sergeant", regex=False)
    )
    df.loc[:, "rank_date"] = rank_date[1]
    return df.drop(columns="date_of_rank")


def assign_agency(df):
    df.loc[:, "data_production_year"] = 2020
    df.loc[:, "agency"] = "harahan-pd"
    return df


def clean():
    return (
        pd.read_csv(deba.data("raw/harahan_pd/harahan_pd_pprr_2020.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={"first": "first_name", "last": "last_name", "badge": "badge_no"}
        )
        .pipe(split_rank_date)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(clean_dates, ["rank_date"])
        .pipe(assign_agency)
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
    )


if __name__ == "__main__":
    df = clean()

    df.to_csv(deba.data("clean/pprr_harahan_pd_2020.csv"), index=False)
