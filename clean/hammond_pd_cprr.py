import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates, standardize_desc_cols
from lib.uid import gen_uid
from lib.rows import duplicate_row
import re


def clean_tracking_number(df):
    df.loc[:, 'tracking_number'] = df.internal_affairs.fillna('').str.lower().str.strip()\
        .str.replace(r'(ia-|ia )', '', regex=True)\
        .str.replace('.', '-', regex=False)\
        .str.replace('2018', '18', regex=False)\
        .str.replace('1801', '18-01', regex=False)
    return df.drop(columns='internal_affairs')


def clean_incident_date(df):
    df.incident_date = df.incident_date.fillna('')\
        .str.replace('7/2010 til 5/2012', '07/01/2010', regex=False)
    return df


def clean_disposition(df):
    df.loc[:, 'disposition'] = df.finding.fillna('').str.lower().str.strip()\
        .str.replace('no action', '', regex=False)
    return df.drop(columns='finding')


def split_name(df):
    names = df.ee_name.str.lower().str.strip().str.extract(r'^(\w+) (\w+)')
    df.loc[:, 'first_name'] = names[0]
    df.loc[:, 'last_name'] = names[1]
    return df.drop(columns='ee_name')


def clean_department_desc(df):
    df.loc[:, 'department_desc'] = df.division.str.lower().str.strip()\
        .str.replace('jailer', 'corrections', regex=False)
    return df.drop(columns=['dept', 'division'])


def clean_action(df):
    df.loc[:, 'action'] = df.final_recommendation.fillna('').str.lower().str.strip()\
        .str.replace(
            'three (3) consecutive shift-days suspenion without pay',
            '3-day suspension without pay', regex=False)\
        .str.replace(
            '5-five consecutive shift-days suspension without pay',
            '5-day suspension without pay', regex=False)\
        .str.replace(
            '| day suspension commence 1/28/17',
            'suspended', regex=False)\
        .str.replace('days suspension 2 days', '2-day suspension', regex=False)\
        .str.replace(r'^(\w+) resigned', 'resigned', regex=True)\
        .str.replace('suspended without pay period of 3 days', '3-day suspension', regex=False)\
        .str.replace('frrom', 'from', regex=False)\
        .str.replace('edwin bergeron 10/2/2019', '', regex=False)\
        .str.replace(r'^(\d+) (\w+)', r'\1-\2', regex=True)\
        .str.replace('no action', '', regex=False)\
        .str.replace('/', '', regex=False)
    return df.drop(columns='final_recommendation')


def combine_charge_columns(df):
    def combine(row):
        txts = []
        if pd.notnull(row.violation_1):
            txts.append('%s' % row.violation_1)
        if pd.notnull(row.violation_2_3):
            txts.append('%s' % row.violation_2_3)
        return '| '.join(txts)
    df.loc[:, 'charges'] = df.apply(combine, axis=1, result_type='reduce')
    df = df.drop(columns=['violation_1', 'violation_2_3'])
    return df


def clean_charges(df):
    df.charges = df.charges.str.lower().str.strip().fillna('')\
        .str.replace(r'^(\d+) (\w+)', r'\1 - \2', regex=True)\
        .str.replace('.', '', regex=False)\
        .str.replace(r' \bgo\b ', ' ', regex=True)\
        .str.replace('/', ' and ', regex=False)\
        .str.replace(r'\(founded\)', '', regex=True)\
        .str.replace(r'^excessive force ', 'excessive force | ', regex=True)\
        .str.replace('omissionn', 'omission', regex=False)\
        .str.replace(r' ?(unbec?o?i?mo?ing) ?( ?(of)? ?(an officer)?)? ?', ' unbecoming ', regex=True)\
        .str.replace('video conduct unbecoming', 'video | conduct unbecoming', regex=False)
    return df


def split_rows_with_multiple_charges(df):
    i = 0
    for idx in df[df.charges.str.contains(" | ")].index:
        s = df.loc[idx + i, "charges"]
        parts = re.split(r"\s*(?:\|)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "charges"] = name
        i += len(parts) - 1
    return df


def realign_action_column(df):
    df.loc[
        (df.tracking_number == '15-10') & (df.incident_month == '10'),
        'action',
    ] = '3-day suspension without pay'

    df.loc[
        (df.tracking_number == '15-09') & (df.incident_month == '10'),
        'action',
    ] = '5-day suspension without pay'
    return df


def drop_rows_without_tracking_number(df):
    return df[df.tracking_number != ''].reset_index(drop=True)


def clean():
    df = pd.read_csv(data_file_path('raw/hammond_pd/hammond_pd_cprr_2015_2020.csv'))\
        .pipe(clean_column_names)
    df = df\
        .pipe(split_name)\
        .pipe(clean_tracking_number)\
        .pipe(clean_incident_date)\
        .pipe(clean_disposition)\
        .pipe(clean_action)\
        .pipe(combine_charge_columns)\
        .pipe(clean_charges)\
        .pipe(split_rows_with_multiple_charges)\
        .pipe(clean_department_desc)\
        .pipe(clean_dates, ['incident_date', 'investigation_start_date'])\
        .pipe(realign_action_column)\
        .pipe(drop_rows_without_tracking_number)\
        .pipe(standardize_desc_cols, ['department_desc', 'action', 'charges'])\
        .pipe(set_values, {
            'agency': 'Hammond PD',
            'data_production_year': '2021'
        })\
        .pipe(gen_uid, ['first_name', 'last_name', 'agency'])\
        .pipe(gen_uid,
              ['uid', 'charges', 'tracking_number', 'disposition'], 'complaint_uid')
    return df


if __name__ == '__main__':
    df = clean()
    ensure_data_dir('clean')
    df.to_csv(data_file_path('clean/cprr_hammond_pd_2015_2020.csv'), index=False)
