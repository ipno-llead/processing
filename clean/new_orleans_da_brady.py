from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid
from lib.path import data_file_path, ensure_data_dir
from lib.clean import (
    clean_names, standardize_desc_cols
)
import pandas as pd
import sys
sys.path.append('../')


def extract_date_from_pib(df):
    tracking = df['pib control#'].str.split("-", 1, expand=True)
    df.loc[:, 'receive_year'] = tracking.loc[:, 0].fillna('')
    df.loc[:, 'partial_tracking_number'] = tracking.loc[:, 1].fillna('')
    df.loc[:, 'tracking_number'] = df.receive_year.str.cat(df.partial_tracking_number, sep='-')
    df = df.drop(columns=['pib control#', 'partial_tracking_number'])
    return df


def combine_rule_and_paragraph(df):
    df.loc[:, 'charges'] = df['allegation classification'].str.cat(df.allegation, sep='; ')\
        .str.replace(r'^; ', '', regex=True).str.replace(r'; $', '', regex=True)
    df = df.drop(columns=['allegation classification', 'allegation'])
    return df


def clean_disposition(df):
    df.loc[:, 'disposition'] = df.disposition.str.lower().str.strip() \
        .str.replace('-', ' | ', regex=False)\
        .str.replace('/', ' | ', regex=False)\
        .str.replace('rui', '', regex=False)\
        .str.replace('dismissal', 'dismissed', regex=False)\
        .str.replace('dui', 'driving under the influence', regex=False)\
        .str.replace('sustained dismissal overturned by 4th circuit',
                     'sustained dismissal overturned by the 4th circuit court', regex=False)\
        .str.replace('sustÃ¤ined', 'sustained', regex=False)
    return df


def clean():
    df = pd.read_csv(data_file_path(
        'new_orleans_da/new_orleans_da_cprr_2021.csv'))
    df = clean_column_names(df)
    df.columns = ['pib control#', 'first name', 'last name', 'allegation classification', 'allegation',
                  'directive', 'finding', 'disposition']
    df = df\
        .rename(columns={
            'first name': 'first_name',
            'last name': 'last_name'
        })\
        .pipe(standardize_desc_cols, ['allegation classification', 'allegation']) \
        .pipe(extract_date_from_pib)\
        .pipe(combine_rule_and_paragraph)\
        .pipe(clean_disposition)\
        .pipe(clean_names, ['first_name', 'last_name'])\
        .pipe(set_values, {
            'data_production_year': 2021,
            'agency': 'New Orleans DA'
        })\
        .pipe(gen_uid, ['agency', 'first_name', 'last_name'])
    return df


if __name__ == '__main__':
    df = clean()
    ensure_data_dir('clean')
    df.to_csv(
        data_file_path('clean/cprr_new_orleans_da_2021.csv'),
        index=False)
