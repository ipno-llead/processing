import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path, ensure_data_dir


def extract_lsu_roster(post):
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    return post[post.agency == 'univ. pd - bossier parish cc']


def clean_agency(df):
    df.loc[:, 'agency'] = df.agency\
        .str.replace('univ. pd - bossier parish cc', 
                     'bossier parish community college police department', regex=False)
    return df


def clean():
    df = pd.read_csv(data_file_path('raw/bossier_parish_community_college_pd/pprr_post_2020_11_06.csv'))
    df = df\
        .pipe(extract_lsu_roster)\
        .pipe(clean_agency)
    return df


if __name__ == '__main__':
    df = clean()
    ensure_data_dir('clean')
    df.to_csv(data_file_path('clean/bossier_parish_community_college_pd_pprr_2020.csv'), index=False)
