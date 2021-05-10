from lib.path import data_file_path, ensure_data_dir
from lib.columns import clean_column_names, set_values
from lib.clean import float_to_int_str, clean_names, standardize_desc_cols, clean_salaries
from lib.uid import gen_uid
from lib import salary
import pandas as pd
import sys
sys.path.append('../')


def assign_agency(df, year):
    df.loc[:, 'agency'] = 'Mandeville PD'
    df.loc[:, 'data_production_year'] = year
    return df


def clean_pprr_20():
    return pd.read_csv(data_file_path('mandeville_pd/mandeville_csd_pprr_2020.csv'))\
        .rename(columns={'annual_salary': 'salary'})\
        .pipe(clean_column_names)\
        .pipe(float_to_int_str, ['term_year', 'term_day', 'term_month', 'hire_year', 'hire_day', 'hire_month'])\
        .pipe(standardize_desc_cols, ['rank_desc'])\
        .pipe(set_values, {'salary_freq': salary.YEARLY})\
        .pipe(clean_salaries, ['salary'])\
        .pipe(assign_agency, 2020)\
        .pipe(clean_names, ['first_name', 'last_name'])\
        .pipe(gen_uid, ['agency', 'badge_no', 'first_name', 'last_name'])


def clean_cprr_19():
    return pd.read_csv(data_file_path('mandeville_pd/mandeville_pd_cprr_2019_byhand.csv'))\
        .pipe(clean_column_names)\
        .rename(columns={
            'title': 'rank_desc'
        })\
        .dropna(axis=1, how='all')\
        .pipe(clean_names, ['last_name'])\
        .pipe(float_to_int_str, ['occur_year', 'occur_month', 'occur_day'])\
        .pipe(standardize_desc_cols, ['rank_desc', 'charges', 'disposition'])\
        .pipe(assign_agency, 2020)\
        .pipe(clean_names, ['last_name'])\
        .pipe(gen_uid, ['agency', 'rank_desc', 'last_name'])\
        .pipe(gen_uid, ['agency', 'tracking_number'], 'complaint_uid')


if __name__ == '__main__':
    pprr = clean_pprr_20()
    cprr = clean_cprr_19()
    ensure_data_dir("clean")
    pprr.to_csv(data_file_path(
        "clean/pprr_mandeville_csd_2020.csv"), index=False)
    cprr.to_csv(data_file_path(
        "clean/cprr_mandeville_pd_2019.csv"), index=False)
