from lib.columns import (
    clean_column_names, set_values
)
from lib.path import data_file_path, ensure_data_dir
from lib.uid import gen_uid
from lib.clean import clean_names, clean_dates, standardize_desc_cols, parse_dates_with_known_format
from lib import salary
import pandas as pd
import numpy as np
import sys
sys.path.insert(0, "../")


def match_schema_2014(df):
    df = clean_column_names(df)
    return df[[
        'orgn_desc', 'last_name', 'first_name', 'birth_date',
        'hire_date', 'title_code', 'title_desc', 'termination_date',
        'pay_prog_start_date', 'annual_salary'
    ]].rename(columns={
        "orgn_desc": "department_desc",
        "title_code": "rank_code",
        "title_desc": "rank_desc",
        "termination_date": "term_date",
        'annual_salary': 'salary'
    }).pipe(set_values, {'salary_freq': salary.YEARLY})


def parse_salary_2014(df):
    df.loc[:, "salary"] = df.salary.str.replace(
        ",", "", regex=False).astype("float64")
    return df


def standardize_rank_2014(df):
    ranks = df[["rank_code", "rank_desc"]].drop_duplicates()\
        .sort_values("rank_code")
    ranks.loc[:, "rank_desc"] = ranks.rank_desc.str.strip().str.lower()
    dup_codes = ranks[ranks.rank_code.duplicated()].rank_code.to_list()
    ranks = ranks.drop(
        index=ranks.loc[(ranks.rank_code.isin(dup_codes)) &
                        (ranks.rank_desc == "DEFAULT")].index
    )
    ranks_map = ranks.set_index("rank_code", drop=True).rank_desc.to_dict()
    df.loc[:, "rank_desc"] = df.rank_code.map(lambda x: ranks_map[x])
    return df


def assign_cols_2014(df):
    df.loc[:, "data_production_year"] = "2014"
    df.loc[:, "agency"] = "New Orleans PD"
    return df


def clean_2014():
    df = pd.read_csv(data_file_path(
        "raw/new_orleans_csd/new_orleans_csd_pprr_2014.csv"))
    df = df\
        .pipe(match_schema_2014)\
        .pipe(
            parse_dates_with_known_format, ['birth_date', 'hire_date', 'term_date', 'pay_prog_start_date'], "%m/%d/%Y")\
        .pipe(clean_names, ["first_name", "last_name"])\
        .pipe(parse_salary_2014)\
        .pipe(standardize_rank_2014)\
        .pipe(standardize_desc_cols, ["department_desc"])\
        .pipe(assign_cols_2014)\
        .pipe(
            gen_uid, 
            ["first_name", "last_name", "hire_year", "hire_month", "hire_day"])
    return df


def match_schema_2009(df):
    df = clean_column_names(df)
    return df[[
        'orgn_desc', 'last_name', 'first_name', 'titl_e_code', 'title_desc',
        'term_date', 'pay_prog_start_date', 'annual_salary'
    ]].rename(columns={
        "orgn_desc": "department_desc",
        "titl_e_code": "rank_code",
        "title_desc": "rank_desc",
        'annual_salary': 'salary',
    }).pipe(set_values, {'salary_freq': salary.YEARLY})


def parse_salary_2009(df):
    df.loc[:, "salary"] = df.salary.str.replace(
        r",|\$", "", regex=True).astype("float64")
    return df


def standardize_rank_2009(df):
    ranks = df.loc[df.rank_code.notna(), ["rank_code", "rank_desc"]]\
        .drop_duplicates().sort_values("rank_code").reset_index(drop=True)
    desc_map = ranks.set_index("rank_desc", drop=True).rank_code.to_dict()
    df.loc[:, "rank_code"] = df.rank_code.where(
        df.rank_code.notna(),
        df.rank_desc.map(lambda x: desc_map.get(x, np.nan))
    )
    df.loc[:, "rank_desc"] = df.rank_desc.str.strip().str.lower()
    return df


def assign_cols_2009(df):
    df.loc[:, "data_production_year"] = "2009"
    df.loc[:, "agency"] = "New Orleans CSD"
    return df


def clean_2009():
    df = pd.read_csv(data_file_path(
        "raw/new_orleans_csd/new_orleans_csd_pprr_2009_realigned.csv"))
    df = df\
        .pipe(match_schema_2009)\
        .pipe(clean_dates, ['term_date', 'pay_prog_start_date'])\
        .pipe(clean_names, ["first_name", "last_name"])\
        .pipe(parse_salary_2009)\
        .pipe(standardize_rank_2009)\
        .pipe(standardize_desc_cols, ["department_desc"])\
        .pipe(
            gen_uid,  # "mid" is match id, used to match against 2014 data
            ["first_name", "last_name", "pay_prog_start_year",
                "pay_prog_start_month", "pay_prog_start_day"],
            "mid")\
        .pipe(assign_cols_2009)
    return df


if __name__ == "__main__":
    df09 = clean_2009()
    df14 = clean_2014()
    ensure_data_dir("clean")
    df09.to_csv(
        data_file_path("clean/pprr_new_orleans_csd_2009.csv"),
        index=False)
    df14.to_csv(
        data_file_path("clean/pprr_new_orleans_csd_2014.csv"),
        index=False)
