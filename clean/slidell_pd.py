import sys
import functools
import operator

import pandas as pd

from lib.columns import clean_column_names, set_values
from lib.path import data_file_path, ensure_data_dir
from lib.uid import gen_uid
from lib.clean import clean_dates, clean_names, clean_salaries, standardize_desc_cols
from lib import salary

sys.path.append('../')


def realign09(df):
    df.loc[:, 'name'] = df.name.fillna(method='ffill')
    return df.dropna(subset=['rank_desc', 'salary_date', 'hire_date', 'salary', 'salary_freq'])


def split_names(df):
    names = df.name.str.lower().str.strip()\
        .str.replace(r' "[^"]+"', '', regex=True)\
        .str.replace(r',? (ii|iii|iv|jr\.?),', r' \1,', regex=True)\
        .str.extract(r'^([^,]+), ([^ ]+)$')
    df.loc[:, 'first_name'] = names[1]
    df.loc[:, 'last_name'] = names[0]
    return df.drop(columns=['name'])


def clean09():
    return pd.read_csv(data_file_path(
        'slidell_pd/slidell_pd_pprr_2009.csv'
    ), delimiter=';')\
        .pipe(clean_column_names)\
        .rename(columns={
            'frequency': 'salary_freq',
            'title': 'rank_desc',
            'effective': 'salary_date',
            'pay': 'salary'
        }).replace({
            'salary_freq': {
                'Hourly': salary.HOURLY,
                'Daily': salary.DAILY,
                'Monthly': salary.MONTHLY,
                'Bi-Weekly': salary.BIWEEKLY
            }
        }).pipe(realign09)\
        .pipe(clean_salaries, ['salary'])\
        .pipe(split_names)\
        .pipe(clean_names, ['first_name', 'last_name'])\
        .reset_index(drop=True)\
        .pipe(clean_dates, ['hire_date', 'salary_date'])\
        .pipe(standardize_desc_cols, ['rank_desc'])\
        .pipe(set_values, {
            'data_production_year': 2009,
            'agency': 'Slidell PD'
        })\
        .pipe(gen_uid, ['agency', 'first_name', 'last_name'])


def realign19(df):
    df = df.dropna(how='all')
    df.columns = ['name'] + df.iloc[0].tolist()[1:]
    df.iloc[:, 0] = df.iloc[:, 0].fillna(method='ffill')
    df = df.dropna(axis=1, how='all')
    return df[~functools.reduce(operator.and_, [
        df[col] == col for col in df.columns[1:8]
    ])].reset_index(drop=True).fillna(method='ffill')


def clean19():
    return pd.read_csv(data_file_path(
        'slidell_pd/slidell_pd_pprr_2019.csv'
    ), delimiter=';')\
        .pipe(realign19)\
        .pipe(clean_column_names)\
        .rename(columns={
            'frequency': 'salary_freq',
            'title': 'rank_desc',
            'effective': 'salary_date',
            'pay': 'salary',
            'division': 'department_desc',
            'badge_number': 'badge_no'
        }).replace({
            'salary_freq': {
                'Hourly': salary.HOURLY,
                'Daily': salary.DAILY,
                'Monthly': salary.MONTHLY,
                'Bi-Weekly': salary.BIWEEKLY
            }
        }).pipe(clean_salaries, ['salary'])\
        .pipe(split_names)\
        .pipe(clean_names, ['first_name', 'last_name'])\
        .pipe(clean_dates, ['hire_date', 'salary_date'])\
        .pipe(standardize_desc_cols, ['rank_desc', 'department_desc', 'employment_status'])\
        .pipe(set_values, {
            'data_production_year': 2019,
            'agency': 'Slidell PD'
        })\
        .pipe(gen_uid, ['agency', 'first_name', 'last_name'])


def fill_empty_columns_for_pprr_2009(df09, df19):
    for _, row in df19.drop_duplicates(subset=['uid']).iterrows():
        for col in ['badge_no', 'department_desc', 'employment_status']:
            df09.loc[df09.uid == row.uid, col] = row[col]
    return df09


if __name__ == '__main__':
    df09 = clean09()
    df19 = clean19()
    df09 = fill_empty_columns_for_pprr_2009(df09, df19)
    df = pd.concat([
        df09,
        df19
    ])
    ensure_data_dir('clean')
    df.to_csv(data_file_path(
        'clean/pprr_slidell_pd_2019.csv'
    ), index=False)
