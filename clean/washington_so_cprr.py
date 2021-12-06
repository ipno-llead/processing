import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path
from lib.columns import clean_column_names, set_values
from lib.clean import standardize_desc_cols, clean_dates
from lib.uid import gen_uid


def clean_allegation(df):
    df.loc[:, 'allegation'] = df.offense.str.lower().str.strip()\
        .str.replace(r' \- ', '; ', regex=True)\
        .str.replace('insubordination/refused', 'insubordination; refused', regex=False)\
        .str.replace(r'\. ', '; ', regex=True)\
        .str.replace('barroom', 'bar room', regex=False)
    return df.drop(columns='offense')


def extract_action_from_disposition(df):
    df.loc[:, 'action'] = df.disposition.str.lower().str.strip()\
        .str.replace(r'( ?unfounded ?| ?sustained ?| ?not sustained ?)', '', regex=True)\
        .str.replace(r'^: ', '', regex=True)\
        .str.replace(r'^\/ ?' , '', regex=True)\
        .str.replace(r'susp (\w+) days?', r'\1-day suspension', regex=True)\
        .str.replace('susp 2 wks. eventually resigned.', '2-week suspension; eventually resigned', regex=False)\
        .str.replace("turned over to lsp & u.s. atty's office: arrested - criminal charges pending in 22nd jdc",
                     r"case turned over to the louisiana state police and u.s attorney's office;"
                     r" arrested; criminal charges are pending in the 22nd judicial district court", regex=True)\
        .str.replace(r'\/', '; ', regex=True)
    return df 


def clean_disposition(df):
    df.loc[:, 'disposition'] = df.disposition.str.lower().str.strip()\
        .str.replace(r'(:| ?\/ ?) ?(.+)', '', regex=True)\
        .str.replace("turned over to lsp & u.s. atty's office", '', regex=False)
    return df 


def clean_complainant_name(df):
    df.loc[:, 'complainant'] = df.name_of_complainant.str.lower().str.strip()\
        .str.replace('danny branch', '', regex=False)\
        .str.replace(r'capt\.', 'captain', regex=True)
    return df.drop(columns='name_of_complainant')


def split_names(df):
    names = df.name_of_accused_deputy_list_separately_if_more.str.lower().str.strip()\
        .str.replace(r'\.', '', regex=True)\
        .str.extract(r'(det|dep)? ?(\w+) (\w+)')

    df.loc[:, 'rank_desc'] = names[0].fillna('')\
        .str.replace('dep', 'deputy', regex=False)\
        .str.replace('det', 'detective', regex=False)
    df.loc[:, 'first_name'] = names[1]
    df.loc[:, 'last_name'] = names[2]
    return df.drop(columns='name_of_accused_deputy_list_separately_if_more')


def clean():
    df = pd.read_csv(data_file_path('raw/washington_so/washington_so_cprr_2015_2020.csv'))\
        .pipe(clean_column_names)\
        .rename(columns={
            'number': 'tracking_number',
            'date': 'receive_date'
        })\
        .pipe(clean_dates, ['receive_date'])\
        .pipe(clean_allegation)\
        .pipe(extract_action_from_disposition)\
        .pipe(clean_complainant_name)\
        .pipe(clean_disposition)\
        .pipe(split_names)\
        .pipe(standardize_desc_cols, ['tracking_number'])\
        .pipe(set_values, {
            'agency': 'Washington SO'
        })\
        .pipe(gen_uid, ['agency', 'first_name', 'last_name'])\
        .pipe(gen_uid, ['uid', 'disposition', 'tracking_number', 'action'], 'allegation_uid')
    return df


if __name__ == '__main__':
    df = clean()
    df.to_csv(data_file_path('clean/cprr_washington_so_2015_2020.csv'), index=False)
