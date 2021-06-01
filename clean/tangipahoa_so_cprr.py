import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.path import data_file_path, ensure_data_dir
from lib.uid import gen_uid
from lib import salary
from lib.clean import (
    clean_names, clean_dates, clean_sexes, clean_races, clean_salaries
)
import sys
sys.path.append('../')


def split_name(df):
    series = df.full_name.fillna('').str.strip()
    for col, pat in [('first_name', r"^([\w'-]+)(.*)$"), ('middle_initial', r'^(\w\.|\w\s)(.*)$')]:
        names = series[series.str.match(pat)].str.extract(pat)
        df.loc[series.str.match(pat), col] = names[0]
        series = series.str.replace(pat, r'\2').str.strip()
    df.loc[:, 'last_name'] = series
    return df.drop(columns=['full_name'])


def clean():
    df = pd.read_csv(data_file_path(
        'tangipahoa_so/tangipahoa_so_cprr_2015-2021.csv'))
    df = clean_column_names(df)
    df.columns = ['receive_date', 'completion_date', 'full_name', 'receive_by', 'dept_desc', 'complainant_type',
                  'rule_violation', "investigating_supervisor", "disposition", "action", "appeal", "level",
                  "policy_failure", "submission_type"]
    df = df\
        .rename(columns={
            'policy_failure': 'term_date'
        })\
        .pipe(split_name)\
        .pipe(set_values, {
            'data_production_year': 2021,
            'agency': 'Scott PD'
        })\
        .pipe(gen_uid, ['agency', 'first_name', 'last_name'])
    return df


if __name__ == '__main__':
    df = clean()
    ensure_data_dir('clean')
    df.to_csv(
        data_file_path('clean/cprr_tangipahoa_so_2021.csv'),
        index=False)
