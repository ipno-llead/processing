from lib.columns import clean_column_names
from lib.path import data_file_path, ensure_data_dir
from lib.clean import clean_dates, clean_names, float_to_int_str, standardize_desc_cols
from lib.uid import gen_uid
import pandas as pd
import sys
sys.path.append('../')


def clean():
    df = pd.read_csv(
        data_file_path('st_tammany_so/st._tammany_so_pprr_2020.csv')
    )
    df = clean_column_names(df)
    df = df.rename(columns={
        'job_class': 'rank_code',
        'job_class_desc': 'rank_desc',
        'year_of_birth': 'birth_year'
    })
    return df\
        .pipe(clean_names, ['first_name', 'last_name'])\
        .pipe(float_to_int_str, ['birth_year'])\
        .pipe(standardize_desc_cols, ['rank_desc'])\
        .pipe(clean_dates, ['hire_date', 'term_date'])\
        .pipe(gen_uid, ['employee_id', 'first_name', 'last_name', 'birth_year'])
