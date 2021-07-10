from lib.columns import clean_column_names, set_values
from lib.path import data_file_path, ensure_data_dir
from lib.clean import (
    parse_dates_with_known_format, clean_salaries, standardize_desc_cols, clean_names
)
from lib.uid import gen_uid
from lib import salary
import pandas as pd
import numpy as np
import sys
sys.path.append('../')


def assign_agency(df):
    df.loc[:, 'agency'] = 'Louisiana State Police'
    df.loc[:, 'data_production_year'] = 2021
    return df


def split_names(df):
    names = df.employee_name.str.strip().str.extract(
        r'^([^,]+),([^ ]+)(?: (\w))?$')
    df.loc[:, 'last_name'] = names.loc[:, 0]
    df.loc[:, 'first_name'] = names.loc[:, 1]
    df.loc[:, 'middle_initial'] = names.loc[:, 2]
    return df.drop(columns=['employee_name'])


def convert_hire_date(df):
    df.loc[:, 'hire_date'] = df.hire_date.astype(str).map(
        lambda x: np.NaN if x == '0' else x)
    return df


def clean():
    return pd.read_csv(data_file_path(
        'louisiana_csd/louisiana_csd_pprr_2021.csv'
    ))\
        .pipe(clean_column_names)\
        .drop(columns=['agency_name', 'data_date'])\
        .pipe(assign_agency)\
        .rename(columns={
            'organizational_unit': 'department_desc',
            'job_title': 'rank_desc',
            'annual_rate_of_pay': 'salary',
            'state_of_la_begin_date': 'hire_date'
        })\
        .pipe(convert_hire_date)\
        .pipe(parse_dates_with_known_format, ['hire_date'], '%Y%m%d')\
        .pipe(set_values, {'salary_freq': salary.YEARLY})\
        .pipe(clean_salaries, ['salary'])\
        .pipe(standardize_desc_cols, ['rank_desc', 'department_desc'])\
        .pipe(split_names)\
        .pipe(clean_names, ['first_name', 'last_name', 'middle_initial'])\
        .pipe(gen_uid, ['agency', 'first_name', 'last_name', 'middle_initial'])


if __name__ == '__main__':
    df = clean()
    ensure_data_dir('clean')
    df.to_csv(data_file_path(
        'clean/pprr_louisiana_csd_2021.csv'
    ), index=False)
