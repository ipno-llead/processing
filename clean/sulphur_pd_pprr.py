import sys

import pandas as pd

from lib.columns import clean_column_names, set_values
from lib.path import data_file_path
from lib.clean import clean_names, clean_officer_inactive, clean_salaries, float_to_int_str, standardize_desc_cols
from lib.uid import gen_uid

sys.path.append('../')


def clean():
    return pd.read_csv(data_file_path(
        'raw/sulphur_pd/sulphur_pd_pprr_2021.csv'
    ), sep=';')\
        .dropna(how='all')\
        .pipe(clean_column_names)\
        .rename(columns={
            'employee': 'employee_id',
            'status_code': 'officer_inactive',
            'middle_init': 'middle_initial',
            'regular_rate': 'salary'
        })\
        .pipe(clean_names, ['first_name', 'middle_initial', 'last_name'])\
        .pipe(clean_salaries, ['salary'])\
        .pipe(float_to_int_str, ['employee_id'])\
        .pipe(clean_officer_inactive, ['officer_inactive'])\
        .pipe(standardize_desc_cols, ['salary_freq'])\
        .pipe(set_values, {
            'agency': 'Sulphur PD'
        }).pipe(gen_uid, ['agency', 'employee_id'])


if __name__ == '__main__':
    df = clean()
    df.to_csv(data_file_path(
        'clean/pprr_sulphur_pd_2021.csv'
    ), index=False)
