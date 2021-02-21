from lib.columns import clean_column_names
from lib.path import data_file_path, ensure_data_dir
from lib.clean import (
    clean_names, clean_dates, standardize_desc_cols
)
from lib.uid import gen_uid
import pandas as pd
import sys
sys.path.append("../")


def standardize_agency(df):
    df.loc[:, "agency"] = df.agency.str.strip()\
        .str.lower().fillna("").str.replace(r"(\w)\.\s*(\w)\.", r"\1\2")
    return df


def clean():
    df = pd.read_csv(data_file_path("post_council/post_pprr_11-6-2020.csv"))
    df = clean_column_names(df)
    df.columns = [
        'agency', 'last_name', 'first_name', 'employment_status',
        'hire_date', 'level_1_cert_date', 'last_pc_12_qualification_date']
    # df = df.drop(
    #     columns=['level_1_cert_date', 'last_pc_12_qualification_date'])
    df = df.dropna(0, "all", subset=["first_name"])
    df = df\
        .pipe(clean_names, ["first_name", "last_name"])\
        .pipe(standardize_agency)\
        .pipe(standardize_desc_cols, ["employment_status"])\
        .pipe(clean_dates, ["hire_date"])\
        .pipe(gen_uid, ['agency', 'last_name', 'first_name', 'hire_year', 'hire_month', 'hire_day'])
    return df
