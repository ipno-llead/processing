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
    names1 = df.name.str.strip().str.replace(
        r"^(\w+(?: \w\.)?) (\w+(?:, \w{2}\.)?)$", r"\1@@\2").str.split("@@", expand=True)
    names2 = names1.iloc[:, 0].str.split(" ", expand=True)
    df.loc[:, "first_name"] = names2.iloc[:, 0]
    df.loc[:, "middle_initial"] = names2.iloc[:, 1]
    df.loc[:, "last_name"] = names1.iloc[:, 1]
    df = df.drop(columns=["name"])
    return df


def clean_action(df):
    df.loc[:, "action"] = df.action.str.lower().str.strip()\
        .str.replace(r"(\. |, | and )", " | ").str.replace(r"\.$", "")\
        .str.replace("privliges", "privileges", regex=False)
    return df


def split_infraction(df):
    infractions = df.infraction.str.extract(r"^([A-Za-z ,]+)(\d.+)?$")
    df.loc[:, 'charges'] = infractions.iloc[:, 1].fillna('').str.strip()\
        .str.replace(r"-(\d+)$", r".\1")\
        .str.cat(
            infractions.iloc[:, 0].str.strip(),
            sep=' - ')\
        .str.replace(r'^ - ', '')
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
            ["rank_desc", "disposition", "complainant_type", "department_desc", "charges"])\
        .pipe(clean_dates, ["rank_date"])\
        .pipe(clean_races, ["race"])\
        .pipe(clean_sexes, ["sex"])\
        .pipe(clean_datetimes, ["occur_datetime"])\
        .pipe(clean_action)\
        .pipe(assign_cols)\
        .pipe(clean_names, ['first_name', 'last_name', 'middle_initial'])\
        .pipe(gen_uid, ["agency", "first_name", "last_name", "birth_year", "badge_no"])\
        .pipe(gen_uid, ['agency', 'uid', 'occur_year', 'occur_month', 'occur_day'], 'complaint_uid')
    return df


if __name__ == "__main__":
    df = clean18()
    ensure_data_dir("clean")
    df.to_csv(
        data_file_path("clean/cprr_baton_rouge_so_2018.csv"),
        index=False)
