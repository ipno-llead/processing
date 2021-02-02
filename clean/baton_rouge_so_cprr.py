from lib.columns import clean_column_names
from lib.uid import gen_uid
from lib.path import data_file_path, ensure_data_dir
from lib.clean import (
    clean_name, standardize_desc_cols, clean_dates, clean_sexes, clean_races, clean_datetimes
)
import pandas as pd
import sys
sys.path.append("../")


def split_name(df):
    names1 = df.name.str.strip().str.replace(
        r"^(\w+(?: \w\.)?) (\w+(?:, \w{2}\.)?)$", r"\1@@\2").str.split("@@", expand=True)
    names2 = names1.iloc[:, 0].str.split(" ", expand=True)
    df.loc[:, "first_name"] = clean_name(names2.iloc[:, 0])
    df.loc[:, "middle_initial"] = clean_name(names2.iloc[:, 1])
    df.loc[:, "last_name"] = clean_name(names1.iloc[:, 1])
    df = df.drop(columns=["name"])
    return df


def clean_action(df):
    df.loc[:, "action"] = df.action.str.lower().str.strip()\
        .str.replace(r"(\. |, | and )", "; ").str.replace(r"\.$", "")\
        .str.replace("privliges", "privileges", regex=False)
    return df


def split_infraction(df):
    rules = df.infraction.str.replace(
        r"^([A-Za-z ,]+)(\d.+)?$", r"\1@@\2").str.split("@@", expand=True)
    df.loc[:, "rule_violation"] = rules.iloc[:, 0].str.strip()
    df.loc[:, "rule_code"] = rules.iloc[:, 1].str.strip().fillna("")
    df = df.drop(columns=["infraction"])
    return df


def assign_cols(df):
    df.loc[:, "data_production_year"] = "2018"
    df.loc[:, "agency"] = "Baton Rouge SO"
    return df


def clean18():
    df = pd.read_csv(data_file_path(
        "baton_rouge_so/baton_rouge_so_cprr_2018.csv"))
    df = clean_column_names(df)
    df.columns = ['name', 'badge_no', 'rank_desc', 'rank_date', 'race', 'sex',
                  'birth_year', 'infraction',
                  'occur_datetime',
                  'complainant_type',
                  'disposition', 'action', 'department_desc']
    df = df\
        .pipe(split_name)\
        .pipe(split_infraction)\
        .pipe(
            standardize_desc_cols,
            ["rank_desc", "disposition", "complainant_type", "department_desc", "rule_violation"])\
        .pipe(clean_dates, ["rank_date"])\
        .pipe(clean_races, ["race"])\
        .pipe(clean_sexes, ["sex"])\
        .pipe(clean_datetimes, ["occur_datetime"])\
        .pipe(clean_action)\
        .pipe(assign_cols)\
        .pipe(gen_uid, ["first_name", "last_name", "birth_year", "badge_no"])
    return df


if __name__ == "__main__":
    df = clean18()
    ensure_data_dir("clean")
    df.to_csv(
        data_file_path("clean/cprr_baton_rouge_so_2018.csv"),
        index=False)
