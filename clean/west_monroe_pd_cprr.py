import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path
from lib.columns import clean_column_names


def extract_accused_badge_number(df):
    badges = df.name.astype(str).str.extract(r'\((\w+)\)')
    df.loc[:, 'badge_number'] = badges[0]
    return df


def split_accused_names(df):
    names = df.name.str.extract(r'^(\w+)\, (\w+)')
    df.loc[:, 'last_name'] = names[0]
    df.loc[:, 'first_name'] = names[1]
    return df.drop(columns='name')


def extract_receiver_badge_number(df):
    badges = df.received_by.astype(str).str.extract(r'\((\w+)\)')
    df.loc[:, 'receiver_badge_number'] = badges[0]
    return df


def split_receiver_names(df):
    names = df.received_by.str.extract(r'(captain)? ?(\w+)\, (\w+)')
    df.loc[:, 'receiver_rank_desc'] = names[0]
    df.loc[:, 'receiver_last_name'] = names[1]
    df.loc[:, 'receiver_first_name'] = names[2]
    return df.drop(columns='received_by')


def clean():
    df = pd.read_csv(data_file_path('raw/west_monroe_pd/cprr_west_monroe_pd_2020_byhand.csv'))\
        .pipe(clean_column_names)\
        .pipe(extract_accused_badge_number)\
        .pipe(split_accused_names)\
        .pipe(extract_receiver_badge_number)\
        .pipe(split_receiver_names)
    return df
