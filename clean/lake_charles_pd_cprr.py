from lib.uid import gen_uid
import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.clean import clean_names
from lib.columns import clean_column_names
from lib.rows import duplicate_row
import re


def clean_charges(df):
    df.loc[:, 'charges'] = df.nature_of_complaint.str.lower().str.strip()\
        .str.replace(r'\. ?', ' ', regex=True)\
        .str.replace(',', '', regex=False)\
        .str.replace(r'(\d+) ', '', regex=True)\
        .str.replace('unsat perform', 'unsatisfactory performance', regex=False)\
        .str.replace('neg of duty', 'neglect of duty', regex=False)\
        .str.replace(r'cond unbecom(ing)?', 'conduct unbecoming', regex=True)\
        .str.replace('unauth force', 'unauthorized force', regex=False)\
        .str.replace('use of dept equip', 'use of department equipment', regex=False)\
        .str.replace('unknown', '', regex=False)
    return df.drop(columns='nature_of_complaint')


def split_rows_with_multiple_charges(df):
    i = 0
    for idx in df[df.charges.str.contains(r'/')].index:
        s = df.loc[idx + i, 'charges']
        parts = re.split(r'\s*(?:/)\s*', s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, 'charges'] = name
        i += len(parts) - 1
    return df


def clean_action(df):
    df.loc[:, 'action'] = df.action_taken.str.lower().str.strip()\
        .str.replace(r'no action ?(tak[eo]n)?', '', regex=True)
    return df.drop(columns=('action_taken'))


def consolidate_action_and_disposition(df):
    df.loc[:, 'action'] = df.action.str.cat(df.disposition, sep='|')\
        .str.replace(r'(not)? ?sustained ?|exonerated ?|unfounded ?|invalid complaint ?', '', regex=True)\
        .str.replace(r'^\|', '', regex=True)\
        .str.replace(r'\|$', '', regex=True)
    return df


def clean_disposition(df):
    df.loc[:, 'disposition'] = df.disposition.str.lower().str.strip()\
        .str.replace('invalid complaint', '', regex=False)\
        .str.replace('suspended', '', regex=False)
    return df 


def split_rows_with_multiple_officers(df):
    i = 0
    for idx in df[df.last_name.str.contains(r"/|,")].index:
        s = df.loc[idx + i, "last_name"]
        parts = re.split(r"\s*(?:/|,)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "last_name"] = name
        i += len(parts) - 1
    return df


def assign_agency(df):
    df.loc[:, "agency"] = "Lake Charles PD"
    return df


def clean():
    df = pd.read_csv(data_file_path('raw/lake_charles/lake_charles_pd_cprr_2020.csv'))
    df = df\
        .pipe(clean_column_names)\
        .rename(columns={
            'date_of_investigation': 'investigation_start_date',
            'name': 'last_name'
        })\
        .pipe(clean_charges)\
        .pipe(split_rows_with_multiple_charges)\
        .pipe(clean_action)\
        .pipe(consolidate_action_and_disposition)\
        .pipe(clean_disposition)\
        .pipe(split_rows_with_multiple_officers)\
        .pipe(clean_names, ['last_name'])\
        .pipe(assign_agency)\
        .pipe(gen_uid, ['last_name', 'agency'])\
        .pipe(gen_uid, ['last_name', 'investigation_start_date', 'charges', 'action'], 'complaint_uid')\
        .drop_duplicates(subset=('complaint_uid'))
    return df 


if __name__ == '__main__':
    df = clean()
    ensure_data_dir('clean')
    df.to_csv(data_file_path('clean/cprr_lake_charles_pd_2020.csv'))
