import sys
sys.path.append('../')
import pandas as pd 
from lib.path import data_file_path, ensure_data_dir
from lib.columns import clean_column_names
from lib.clean import clean_date, clean_dates


def clean_tracking_number(df):
    df.loc[:, 'tracking_number'] = df.internal_affairs.fillna('').str.lower().str.strip()\
        .str.replace(r'(ia-|ia )', '', regex=True)\
        .str.replace('.', '-', regex=False)\
        .str.replace('2018', '18', regex=False)\
        .str.replace('1801', '18-01', regex=False)
    return df.drop(columns='internal_affairs')


# def clean_incident_date(df):
#     df.incident_date = df.incident_date\
#         .str.replace('7/2010 til 5/2012', '07/01/2010', regex=False)
#     return df


# def clean_disposition(df):
#     df.disposition = df.disposition.fillna('').str.lower().str.strip()\
#         .str.replace('founded/preventable class 2', 'founded | preventable class 2', regex=False)\
#         .str.replace('unfounded/unsubstantiated', 'unfounded | unsubstantiated', regex=False)\
#         .str.replace('no action', '', regex=False)
#     return df

def split_name(df):
    names = df.ee_name.str.lower().str.strip().str.extract(r'^(\w+) (\w+)')
    df.loc[:, 'first_name'] = names[0]
    df.loc[:, 'last_name'] = names[1]
    return df.drop(columns='ee_name')


def clean():
    df = pd.read_csv(data_file_path('hammond_pd/hammond_pd_cprr_2015_2020.csv'))\
        .pipe(clean_column_names)
    df = df\
        .rename(columns={
            'dept': 'department_desc',
        })\
        .pipe(split_name)\
        .pipe(clean_tracking_number)
    return df


if __name__ == '__main__':
    df = clean()
    ensure_data_dir('clean')
    df.to_csv(data_file_path('clean/cprr_hammond_pd_2015_2020.csv'), index=False)
