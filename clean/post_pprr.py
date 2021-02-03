from lib.columns import clean_column_names
from lib.path import data_file_path, ensure_data_dir
from lib.clean import (
    clean_names, clean_dates, standardize_desc_cols
)
import pandas as pd
import sys
sys.path.append("../")


def standardize_department_desc(df):
    df.loc[:, "department_desc"] = df.department_desc.str.strip()\
        .str.lower().fillna("").str.replace(r"(\w)\.\s*(\w)\.", r"\1\2")
    return df


def assign_agency(df):
    df.loc[:, "agency"] = "Peace Officer Standards and Training Council"
    df.loc[:, "data_production_year"] = "2020"
    return df


def clean():
    df = pd.read_csv(data_file_path("post_council/post_pprr_11-6-2020.csv"))
    df = clean_column_names(df)
    df.columns = [
        "department_desc", 'last_name', 'first_name', 'employment_status',
        'hire_date', 'level_1_cert_date', 'last_pc_12_qualification_date']
    # df = df.drop(
    #     columns=['level_1_cert_date', 'last_pc_12_qualification_date'])
    df = df.dropna(0, "all", subset=["first_name"])
    df = df\
        .pipe(clean_names, ["first_name", "last_name"])\
        .pipe(standardize_department_desc)\
        .pipe(standardize_desc_cols, ["employment_status"])\
        .pipe(clean_dates, ["hire_date"])\
        .pipe(assign_agency)
    return df
