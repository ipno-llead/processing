import sys

sys.path.append("../")
from lib.path import data_file_path
import pandas as pd
from lib.columns import clean_column_names
from lib.clean import clean_races, clean_sexes, clean_dates, standardize_desc_cols, clean_names
from lib.uid import gen_uid
from lib.columns import set_values


def clean_rank_desc(df):
    df.loc[:, "rank_desc"] = (
        df.current_rank.str.lower()
        .str.strip()
        .str.replace(r"asst\.", "assistant", regex=True)
        .str.replace(r"comm\.", "communications", regex=True)
    )
    return df.drop(columns="current_rank")


def clean_resignation_date(df):
    df.loc[:, "resignation_date"] = (
        df.resignation_date.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("elected", "", regex=False)
    )
    return df


def split_names(df):
    names = (
        df.name.str.lower()
        .str.strip()
        .fillna("")
        .str.extract(r"(\w+)\, ?(\w+)? (\w+\'?\w+?)$")
    )
    df.loc[:, "last_name"] = names[0]
    df.loc[:, "middle_name"] = names[1].fillna("")
    df.loc[:, "first_name"] = names[2]
    return df.drop(columns="name")


def clean():
    df = (
        pd.read_csv(data_file_path("raw/baker_pd/baker_csd_pprr_2010_2020.csv"))
        .pipe(clean_column_names)
        .drop(columns=["annual_salary", "monthly_salary", "hourly_salary"])
        .rename(columns={"date_of_hire": "hire_date"})
        .pipe(clean_rank_desc)
        .pipe(clean_resignation_date)
        .pipe(clean_dates, ["hire_date", "resignation_date"])
        .pipe(standardize_desc_cols, ["race", "sex"])
        .pipe(clean_races, ["race"])
        .pipe(clean_sexes, ["sex"])
        .pipe(split_names)
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(set_values, {"agency": "Baker PD"})
        .pipe(gen_uid, ["first_name", "middle_name", "last_name"])
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(data_file_path("clean/pprr_baker_pd_2010_2020.csv"), index=False)
