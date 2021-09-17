import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path, ensure_data_dir


def extract_roster(df):
    return df[df.agency == 'univ. pd - dillard']


def clean_agency(df):
    df.loc[:, 'agency'] = df.agency\
        .str.replace('univ. pd - dillard', 
                     'Dillard University PD', regex=False)
    return df


def clean():
    df = pd.read_csv(data_file_path('raw/dillard_university_pd/pprr_post_2020_11_06.csv'))
    df = df\
        .pipe(extract_roster)\
        .pipe(clean_agency)
    return df


if __name__ == '__main__':
    df = clean()
    ensure_data_dir('clean')
    df.to_csv(data_file_path('clean/pprr_dillard_university_pd_2020.csv'), index=False)