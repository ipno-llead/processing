import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path, ensure_data_dir


def extract_roster(df):
    return df[df.agency == 'univ. pd - ull']


def clean_agency(df):
    df.loc[:, 'agency'] = df.agency\
        .str.replace('univ. pd - ull',
                     'University of Louisiana at Lafayette PD', regex=False)
    return df


def clean():
    df = pd.read_csv(data_file_path('raw/university_of_louisiana_lafayette_pd/pprr_post_2020_11_06.csv'))
    df = df\
        .pipe(extract_roster)\
        .pipe(clean_agency)
    return df


if __name__ == '__main__':
    df = clean()
    ensure_data_dir('clean')
    df.to_csv(data_file_path('clean/pprr_university_of_louisiana_lafayette_pd_2020.csv'), index=False)