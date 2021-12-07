import sys
sys.path.append('../')
from lib.path import data_file_path
import pandas as pd
from lib.columns import set_values
from lib.clean import standardize_desc_cols, clean_dates
from lib.uid import gen_uid


def clean_allegation(df):
    df.loc[:, 'allegation'] = df.allegation\
        .str.replace('2.04', '2.04:', regex=False)
    return df


def clean():
    df = pd.read_csv(data_file_path('raw/terrebonne_so/terrebonne_so_cprr_2019_2021_byhand.csv'))\
        .rename(columns={
            'diposition_date': 'disposition_date',
            'final_action': 'action',
            'investigation_end_date': 'investigation_complete_date'
        })\
        .pipe(clean_allegation)\
        .pipe(standardize_desc_cols, ['allegation'])\
        .pipe(clean_dates, ['investigation_start_date', 'investigation_complete_date', 'disposition_date'])\
        .pipe(set_values, {
            'agency': 'Terrebonne Parish SO'
        })\
        .pipe(gen_uid, ['agency', 'first_name', 'last_name'])\
        .pipe(gen_uid, ['uid', 'allegation', 'disposition', 'initial_action', 'action'], 'allegation_uid')
    return df


if __name__ == '__main__':
    df = clean()
    df.to_csv(data_file_path('clean/cprr_terrebonne_so_2019_2021.csv'), index=False)
