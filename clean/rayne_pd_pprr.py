import pandas as pd
import deba
from lib.columns import clean_column_names, set_values
from lib.clean import (
    clean_names,
    clean_races,
    clean_sexes,
    clean_dates,
)
from lib.uid import gen_uid


def clean_birth_year(df):
    df.loc[:, "birth_year"] = df.birth_year.str.replace(
        r"(-|!|\/| +)", "", regex=True
    ).str.replace(r"^(\w{2})$", r"19\1", regex=True)
    return df


def clean_rank_desc(df):
    df.loc[:, "rank_desc"] = (
        df.rank_desc.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"(conf|pro[vb]|cmo) ?p?", "", regex=True)
        .str.replace("sgt", "sergeant", regex=False)
        .str.replace("lt", "lieutenant", regex=False)
        .str.replace(r"p?co", "communications officer", regex=True)
        .str.replace(r"^of?r?c?l?$", "officer", regex=True)
        .str.replace(r"^ca$", "captain", regex=True)
    )
    return df


def clean_left_reason(df):
    df.loc[:, "left_reason"] = (
        df.left_reason.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"^resignation$", "resigned", regex=True)
        .str.replace("retirement", "retired", regex=False)
        .str.replace("termination", "terminated", regex=False)
    )
    return df


def clean():
    df = (
        pd.read_csv(deba.data("raw/rayne_pd/rayne_pd_pprr_2010_2020.csv"))
        .pipe(clean_column_names)
        .pipe(clean_birth_year)
        .pipe(clean_rank_desc)
        .pipe(clean_left_reason)
        .pipe(clean_races, ["race"])
        .pipe(clean_sexes, ["sex"])
        .pipe(clean_dates, ["hire_date", "left_date"])
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(set_values, {"agency": "rayne-pd"})
        .pipe(gen_uid, ["first_name", "middle_name", "last_name", "agency"])
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/pprr_rayne_pd_2010_2020.csv"), index=False)
