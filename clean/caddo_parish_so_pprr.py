from lib.columns import clean_column_names
from lib.path import data_file_path, ensure_data_dir
from lib.clean import clean_names, clean_salaries, standardize_desc_cols, clean_dates
from lib.uid import gen_uid, ensure_uid_unique
import pandas as pd
import sys
sys.path.append('../')


def extract_name(df):
    df2 = df.name.str.strip().str.lower().str.extract(r'^([^,]+), (.+)$')
    df2.columns = ['last_name', 'rest']
    df2 = pd.concat([
        df2.iloc[:, 0],
        df2.iloc[:, 1].str.extract(
            r'^([^ ]+)(?: (.+))?$').rename(columns={0: 'first_name', 1: 'middle_name'})
    ], axis=1)
    rows_with_suffix = df2.middle_name.fillna(
        '').str.match(r'^(?:(.+) )?(jr|sr|ii|iii|iv|v|vi)$')
    df3 = df2.loc[rows_with_suffix, 'middle_name'].str.extract(
        r'^(?:(.+) )?(jr|sr|ii|iii|iv|v|vi)$')
    df2.loc[rows_with_suffix, 'middle_name'] = df3.loc[rows_with_suffix, 0]
    df2.loc[rows_with_suffix, 'last_name'] = df2.loc[rows_with_suffix,
                                                     'last_name'].str.cat(df3.loc[rows_with_suffix, 1], sep=' ')
    df.loc[:, 'first_name'] = df2.first_name
    df.loc[:, 'last_name'] = df2.last_name
    df.loc[:, 'middle_name'] = df2.middle_name
    df.loc[:, 'middle_initial'] = df2.middle_name.fillna('')\
        .map(lambda x: x[:1])
    return df.drop(columns=['name'])


def assign_agency(df):
    df.loc[:, 'agency'] = 'Caddo Parish SO'
    df.loc[:, 'data_production_year'] = 2020
    return df


def clean():
    return pd.read_csv(data_file_path(
        'caddo_parish_so/caddo_parish_so_pprr_2020.csv'
    ))\
        .dropna(axis=1, how='all')\
        .drop_duplicates(ignore_index=True)\
        .pipe(clean_column_names)\
        .rename(columns={
            'yearly_salary': 'annual_salary',
            'rank': 'rank_desc',
            'ee_number': 'employee_id'
        })\
        .drop(columns=['commission_number'])\
        .pipe(extract_name)\
        .pipe(clean_salaries, ['annual_salary'])\
        .pipe(standardize_desc_cols, ['rank_desc'])\
        .pipe(assign_agency)\
        .pipe(clean_dates, ['hire_date'])\
        .pipe(clean_names, ['first_name', 'last_name', 'middle_name'])\
        .pipe(gen_uid, ['agency', 'first_name', 'last_name', 'middle_name', 'employee_id'])


if __name__ == '__main__':
    df = clean()
    ensure_uid_unique(df, 'uid')
    ensure_data_dir('clean')
    df.to_csv(data_file_path(
        'clean/pprr_caddo_parish_so_2020.csv'
    ), index=False)
