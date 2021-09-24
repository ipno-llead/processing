import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path
from lib.columns import clean_column_names
from lib.clean import clean_dates, float_to_int_str


def clean_disposition(df):
    df.loc[:, 'disposition'] = df.disposition.str.lower().str.strip().fillna('')\
        .str.replace('susp sent', 'suspended sentence', regex=False)\
        .str.replace('disposition', '', regex=False)
    return df 


def clean_court_type(df):
    df.loc[:, 'court_type'] = df.court_type.str.lower().str.strip().fillna('')\
        .str.replace('cdc', 'civil district court', regex=False)\
        .str.replace('mun', 'municipal', regex=False)\
        .str.replace('paroled', '', regex=False)\
        .str.replace('0', '', regex=False)
    return df


def clean_court_date(df):
    df.loc[:, 'court_date'] = df.court_date.str.lower()\
        .str.replace(r'(\d+)/(\d+)/(\d+)', r'\2/\3/\1', regex=True)\
        .str.replace('1/14/2', '', regex=False)\
        .str.replace(r'^(\w+)$', '', regex=True)\
        .str.replace(r'^(\d+)$', '', regex=True)
    return df


def clean_sentence_date(df):
    df.loc[:, 'sentence_date'] = df.sentence_date.str.lower()\
        .str.replace(r'(\d+)/(\d+)/(\d+)', r'\2/\3/\1')\
        .str.replace(r'^(\d+)$', '', regex=True)\
        .str.replace(r'(\d+)/(\d{4})/(\d+)', r'\3/\1/\2', regex=True)
    return df


def clean_court_time(df):
    df.loc[:, 'am_pm'] = df.court_time_flag_am_pm.str.lower().fillna('')\
        .str.replace(r'8?0?0', '', regex=True)\
        .str.replace('seventh district', '', regex=False)

    df.loc[:, 'court_time'] = df.court_time.str.lower().fillna('')\
        .str.replace('third district', '', regex=False)\
        .str.replace('police attorney', '', regex=False)\
        .str.replace("criminal sheriff's office", '', regex=False)\
        .str.replace('17/08/03', '', regex=False)\
        .str.replace('first district', '', regex=False)\
        .str.replace(r'^0$', '', regex=True)\
        .str.replace(r'^(\d{1})$', r'\1:00', regex=True)\
        .str.replace(r'^(\d{1})(\d{1})(\d{1})$', r'\1:\2\3', regex=True)\
        .str.replace(r'^(\d{1})(\d{1})(\d{1})(\d{1})$', r'\1\2:\3\4', regex=True)

    df.loc[:, 'court_time'] = df.court_time.str.cat(df.am_pm)\
        .str.replace(r'^(\w+)$', '', regex=True)
    return df.drop(columns=['court_time_flag_am_pm', 'am_pm'])


def clean_arresting_department(df):
    df.loc[:, 'arresting_department'] = df.arrest_credit_txt.str.lower().str.strip()\
        .str.replace(r'^(\d+)$', '', regex=True)\
        .str.replace('environmental rangers = ', '', regex=False)\
        .str.replace('sanitatn', 'sanitation', regex=False)\
        .str.replace('dept', 'department', regex=False)\
        .str.replace('grant: ', '', regex=False)
    return df


def clean():
    dfa = pd.read_csv(data_file_path('raw/ipm/new_orleans_so_charges_2015_2021.csv'))\
        .pipe(clean_column_names)
    dfb = pd.read_csv(data_file_path('raw/ipm/new_orleans_so_bookings_2015_2021.csv'))\
        .pipe(clean_column_names)
    
    df = pd.merge(dfa, dfb, on='folder_no', how='outer')\
        .rename(columns={
            'sentence_yrs': 'sentence_years',
            'sentence_mos': 'sentence_months',
            'sentence_dys': 'sentence_days',
            'sentence_oth': 'sentence_other'
        })\
        .pipe(float_to_int_str, ['arrest_credit'])\
        .pipe(clean_court_date)\
        .pipe(clean_sentence_date)\
        .pipe(clean_dates, ['court_date', 'sentence_date'])\
        .pipe(clean_disposition)\
        .pipe(clean_court_type)\
        .pipe(clean_court_time)\
        .pipe(clean_arresting_department)

    return df
