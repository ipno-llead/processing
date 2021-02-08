from lib.columns import clean_column_names
from lib.path import data_file_path, ensure_data_dir
from lib.clean import float_to_int_str, clean_dates, clean_names
from lib.uid import gen_uid
from lib.rows import duplicate_row
import pandas as pd
import numpy as np
import sys
sys.path.append("../")


def split_names(df):
    names = df.name.str.split(" ", expand=True)
    df.loc[:, "first_name"] = names.loc[:, 0]
    df.loc[:, "last_name"] = names.loc[:, 1]
    df = df.drop(columns="name")
    return df


def split_rows_by_salary(df):
    salary_cols = [
        '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019']
    df.loc[:, "annual_salary"] = np.nan
    idx = 0
    while idx < df.shape[0]:
        row = df.loc[idx]
        salaries = dict()
        for col in salary_cols:
            if not pd.isna(row[col]):
                salaries[col] = row[col]
        salaries_count = len(salaries)
        if salaries_count == 0:
            idx += 1
            continue
        elif salaries_count > 1:
            df = duplicate_row(df, idx, salaries_count)
        j = 0
        for k, v in salaries.items():
            df.loc[idx+j, "annual_salary"] = v
            df.loc[idx+j, "data_production_year"] = k
            j += 1
        idx += salaries_count
    # df = df.drop(columns=salary_cols)
    # df.loc[:, "annual_salary"] = df.annual_salary.astype("float64")
    return df


def clean():
    df = pd.read_csv(data_file_path(
        "madisonville_pd/madisonville_csd_pprr_2021.csv"))
    df = clean_column_names(df)
    df.columns = [
        'name', 'badge_no', 'hire_date', '2012', '2013', '2014', '2015',
        '2016', '2017', '2018', '2019']
    df = df\
        .pipe(float_to_int_str, ["badge_no"])\
        .pipe(split_names)\
        .pipe(clean_names, ["first_name", "last_name"])\
        .pipe(clean_dates, ["hire_date"])\
        .pipe(gen_uid, ["first_name", "last_name", "badge_no"])
    return df
