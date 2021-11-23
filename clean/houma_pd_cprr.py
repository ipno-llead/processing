import sys
sys.path.append('../')
from lib.path import data_file_path
import pandas as pd
from lib.columns import clean_column_names
from lib.clean import standardize_desc_cols
from lib.columns import set_values
from lib.rows import duplicate_row
import re
from lib.uid import gen_uid


def split_rows_with_multiple_allegations(df):
    df.loc[:, 'allegation'] = df.policy_violation\
        .str.replace(r' 7 ', '/', regex=False)\
        .str.replace(r' ?\/ ?', r'/', regex=True)\
        .str.replace(r'\*', '', regex=True)

    i = 0
    for idx in df[df.allegation.str.contains('/')].index:
        s = df.loc[idx + i, 'allegation']
        parts = re.split(r"\s*(?:\/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, 'allegation'] = name
        i += len(parts) - 1
    return df.drop(columns='policy_violation')


def clean_allegations(df):
    df.loc[:, 'allegation'] = df.allegation.str.lower().str.strip()\
        .str.replace('use/possessission of drugs prohibited',
                     'use or posession of prohibited drugs', regex=False)\
        .str.replace(r'article (\w+) (\w+)', r'article \1: \2', regex=True)\
        .str.replace(r'^neglect of [do]uty$', 'article 64: neglect of duty', regex=True)\
        .str.replace('dept', 'department', regex=False)\
        .str.replace(r'fit(ness)? for [od]uty', 'fitness for duty', regex=True)\
        .str.replace(r'(\w+)\.', r'\1,', regex=True)\
        .str.replace('officier', 'officer', regex=False)\
        .str.replace(r'^sick leave$', 'sick leave policy', regex=True)\
        .str.replace(r' \.$', '', regex=True)\
        .str.replace('lethat', 'lethal', regex=False)\
        .str.replace(r'^operation department vehicles', 'article 105: operation of department vehicles', regex=True)
    return df


def extract_action(df):
    df.loc[:, 'action'] = df.inv_disposition.str.lower().str.strip()\
        .str.replace(r' 7', '', regex=False)\
        .str.replace(r'((not)? ?sust[aä]n?ined|(all officers)? ?exonerated|unfounded|'
                     r'suspended investigation)', '', regex=True)\
        .str.replace('no discipline', '', regex=False)\
        .str.replace(r'^ ?\/ ?', '', regex=True)\
        .str.replace(' calendar ', ' ', regex=False)\
        .str.replace(r'suspenst?i?on \/? ?(.+)', r'suspension; \1', regex=True)\
        .str.replace(r'reprimand ?\/? ?(.+)', r'reprimand; \1', regex=True)\
        .str.replace(r' \/$', '', regex=True)\
        .str.replace(r';$', '', regex=True)\
        .str.replace('video/ audio', 'a/v', regex=False)\
        .str.replace(r' ti day ', '', regex=False)\
        .str.replace(r'(\w+) days?', r'\1-day', regex=True)\
        .str.replace('equipment ', '', regex=False)
    return df


def clean_disposition(df):
    df.loc[:, 'disposition'] = df.inv_disposition.str.lower().str.strip()\
        .str.replace(r'^sust[aä]n?ined ?\/? ?(.+)', r'sustained', regex=True)\
        .str.replace(r'(all officers |equipment|verbal counseling)', '', regex=True)\
        .str.replace(r'^written (.+)', '', regex=True)\
        .str.replace(r'(.+)? ?(suspension) ?(.+)?', '', regex=True)
    return df.drop(columns='inv_disposition')


def split_and_clean_investigator(df):
    investigators = df.investigator_column6_column5_column4_column3_column2_columnt.str.lower().str.strip()\
        .str.replace(r'Sgt\.Dave Wagner ', 'sgt david wagner', regex=True)\
        .str.replace('jarrad', 'jarrod', regex=False)\
        .str.replace('magtherne', 'matherne', regex=False)\
        .str.extract(r'(sgt)\.? (\w+) (\w+)')

    df.loc[:, 'investigator_rank_desc'] = investigators[0]\
        .str.replace('sgt', 'sergeant', regex=False)
    df.loc[:, 'investigator_first_name'] = investigators[1]
    df.loc[:, 'investigator_last_name'] = investigators[2]
    return df.drop(columns='investigator_column6_column5_column4_column3_column2_columnt')


def clean_ia_tracking_number(df):
    df.loc[:, 'external_tracking_number'] = df.pd_case_number.str.lower().str.strip()\
        .str.replace(r'pd(.+)', r'pd-\1', regex=True)\
        .str.replace(r'lsp(.+)', r'lsp-\1', regex=True)
    return df.drop(columns='pd_case_number')


def split_and_clean_names(df):
    df.loc[:, 'officer'] = df.officer.str.lower().str.strip()\
        .str.replace('sidneytherlot', 'sidney therlot', regex=False)\
        .str.replace(r'(polk street ois|houma police dept)', '', regex=True)

    names = df.officer.str.extract(r'(\w+) (\w+)')

    df.loc[:, 'first_name'] = names[0].fillna('')
    df.loc[:, 'last_name'] = names[1].fillna('')
    return df.drop(columns='officer')


def drop_rows_missing_first_and_last_name(df):
    return df[~((df.first_name == '') & (df.last_name == ''))]


def clean():
    df = pd.read_csv(data_file_path('raw/houma_pd/houma_pd_cprr_2019_2021.csv'))\
        .pipe(clean_column_names)\
        .rename(columns={
            'case_number': 'tracking_number'
        })\
        .pipe(split_rows_with_multiple_allegations)\
        .pipe(clean_allegations)\
        .pipe(clean_ia_tracking_number)\
        .pipe(extract_action)\
        .pipe(clean_disposition)\
        .pipe(split_and_clean_investigator)\
        .pipe(split_and_clean_names)\
        .pipe(drop_rows_missing_first_and_last_name)\
        .pipe(standardize_desc_cols, ['tracking_number', 'action'])\
        .pipe(set_values, ({'agency': 'Houma PD'}))\
        .pipe(gen_uid, ['agency', 'first_name', 'last_name'])\
        .pipe(gen_uid, ['uid', 'tracking_number', 'external_tracking_number', 'allegation', 'action'], 'complaint_uid')
    return df


if __name__ == '__main__':
    df = clean()
    df.to_csv(data_file_path('clean/cprr_houma_pd_2019_2021.csv'))
