from lib.columns import clean_column_names, set_values
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
    outcomes = df["disposition_action"].str.split("/", expand=True)
    df.loc[:, "disposition"] = outcomes.loc[:, 0].fillna("")
    df.loc[:, "action"] = outcomes.loc[:, 1].fillna("")
    df = df.drop(columns="disposition_action")
    return df


def clean_rank(df):
    df.loc[:, "rank_desc"] = df.rank_desc.str.lower().str.strip() \
        .str.replace("pofc", "officer", regex=False)\
        .str.replace("lt.", "lieutenant ", regex=False)
    return df


def clean_charges(df):
    df.loc[:, "charges"] = df.rule_violation.str.lower().str.strip() \
        .str.replace("conduct unbecoming/ rude behavior", "conduct unbecoming (rude behavior)", regex=False)\
        .str.replace("conduct unbecoming/lying", "conduct unbecoming (lying)", regex=False)\
        .str.replace("conduct unbecoming/rudeness", "conduct unbecoming (rudeness)", regex=False)
    df = df.drop(columns='rule_violation')
    return df


def clean():
    df = pd.read_csv(data_file_path(
        "dropbox/scott_pd/scott_pd_cprr_2020.csv")
    ).pipe(clean_column_names)\
        .drop(columns="appeal")\
        .dropna()\
        .pipe(split_name)\
        .pipe(
            standardize_desc_cols,
            ["rank_desc", "disposition_action"])\
        .pipe(clean_charges)\
        .pipe(clean_rank)\
        .pipe(set_values, {
            'data_production_year': 2020,
            'agency': 'Scott PD'})\
        .pipe(split_disposition_action)\
        .pipe(clean_dates, ["notification_date"]) \
        .rename(columns={
            'notification_year': 'receive_year',
            'notification_month': 'receive_month',
            'notification_day': 'receive_day'})\
        .pipe(clean_names, ["first_name", "last_name"])\
        .pipe(gen_uid, ["agency", "first_name", "last_name"])\
        .pipe(gen_uid, ["agency", "uid", "receive_year", "receive_month", "receive_day"], "complaint_uid")
    return df


if __name__ == "__main__":
    df = clean()
    ensure_data_dir("clean")
    df.to_csv(
        data_file_path("clean/cprr_scott_pd_2020.csv"),
        index=False)
