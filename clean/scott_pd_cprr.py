from lib.columns import clean_column_names
from lib.uid import gen_uid
from lib.path import data_file_path, ensure_data_dir
from lib.clean import (
    clean_names, standardize_desc_cols, clean_dates
)
import pandas as pd
import sys
sys.path.append("../")

def split_name(df):
    names = df.full_name.str.split(" ", expand=True)
    df.loc[:, "rank_desc"] = names.loc[:, 0].fillna("")
    df.loc[:, "first_name"] = names.loc[:, 1].fillna("")
    df.loc[:, "last_name"] = names.loc[:, 2].fillna("")
    df = df.drop(columns="full_name")
    return df

def split_disposition_action(df):
    outcomes = df["disposition/action"].str.split("/", expand=True)
    df.loc[:, "disposition"] = outcomes.loc[:, 0].fillna("")
    df.loc[:, "action"] = outcomes.loc[:, 1].fillna("")
    df = df.drop(columns="disposition/action")
    return df

def clean_rank(df):
    df.loc[:, "rank_desc"] = df.rank_desc.str.lower().str.strip() \
        .str.replace("pofc", "officer", regex=False)\
        .str.replace("lt.", "lieutenant ", regex=False)
    return df

def assign_agency(df):
    df.loc[:, "agency"] = "Scott PD"
    return df

def assign_prod_year(df, year):
    df.loc[:, "data_production_year"] = year
    return df

def clean_charges(df):
    df.loc[:, "charges"] = df.charges.str.lower().str.strip() \
        .str.replace("conduct unbecoming/ rude behavior", "conduct unbecoming (rude behavior)", regex=False)\
        .str.replace("conduct unbecoming/lying", "conduct unbecoming (lying)", regex=False)\
        .str.replace("conduct unbecoming/rudeness", "conduct unbecoming (rudeness)", regex=False)
    return df

def clean():
    df = pd.read_csv(data_file_path(
        "scott_pd/scott_pd_cprr_2020.csv"))
    df = clean_column_names(df) \
        .rename(columns={
        'rule_violation': 'charges'})
    df.columns = ['notification_date', 'full_name', 'charges', 'appeal', 'disposition/action']
    df = df.drop(columns=["appeal"])
    df = df\
        .pipe(split_name)\
        .pipe(
            standardize_desc_cols,
            ["rank_desc", "disposition/action"])\
        .pipe(assign_prod_year, "2020")\
        .pipe(assign_agency)\
        .pipe(clean_charges)\
        .pipe(clean_rank)\
        .pipe(split_disposition_action)\
        .pipe(clean_dates, ["notification_date"])\
        .pipe(clean_names, ["first_name", "last_name"])\
        .pipe(gen_uid, ["agency", "first_name", "last_name"])\
        .pipe(gen_uid, ["agency", "uid", "notification_year", "notification_month", "notification_day"], "complaint_uid")
    return df

if __name__ == "__main__":
    df = clean()
    ensure_data_dir("clean")
    df.to_csv(
        data_file_path("clean/cprr_scott_pd_2020.csv"),
        index=False)