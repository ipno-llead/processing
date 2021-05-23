from lib.columns import clean_column_names
from lib.uid import gen_uid
from lib.path import data_file_path, ensure_data_dir
from lib.clean import (
    clean_names, standardize_desc_cols, clean_dates, clean_sexes, clean_races, clean_datetimes
)
import pandas as pd
import sys
sys.path.append("../")

def split_name(df):
    names1 = df.full_name.str.strip().str.replace(
        r"^(\w+(?: \w\.)?) (\w+(?:, \w{2}\.)?)$", r"\1@@\2", regex=False).str.split("@@", expand=True)
    names2 = names1.iloc[:, 0].str.split(" ", expand=True)
    df.loc[:, "rank_desc"] = names2.iloc[:, 0]
    df.loc[:, "first_name"] = names2.iloc[:, 1]
    df.loc[:, "last_name"] = names1.iloc[:, 0]
    df = df.drop(columns=["full_name"])
    return df

def assign_agency(df):
    df.loc[:, "agency"] = "Scott PD"
    return df

def assign_prod_year(df, year):
    df.loc[:, "data_production_year"] = year
    return df


def clean():
    df = pd.read_csv(data_file_path(
        "scott_pd/scott_pd_cprr_2020.csv"))
    df = clean_column_names(df)
    df.columns = ['notification_date', 'full_name', 'rule_violation', 'appeal', 'disposition/action']
    df = df\
        .pipe(split_name)\
        .pipe(
            standardize_desc_cols,
            ["rank_desc", "disposition/action"])\
        .pipe(assign_prod_year, '2020')\
        .pipe(assign_agency)\
        .pipe(clean_names, ['first_name', 'last_name'])\
        .pipe(gen_uid, ["agency", "first_name", "last_name"])\
        .pipe(gen_uid, ['agency', 'uid'], 'complaint_uid')
    return df

if __name__ == "__main__":
    df = clean()
    ensure_data_dir("clean")
    df.to_csv(
        data_file_path("clean/cprr_scott_pd_2020.csv"),
        index=False)