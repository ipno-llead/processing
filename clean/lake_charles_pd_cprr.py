import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.clean import clean_names, float_to_int_str
from lib.columns import clean_column_names
from lib.rows import duplicate_row
from lib.uid import gen_uid
import re


def clean_charges(df):
    df.loc[:, 'charges'] = df.nature_of_complaint.str.lower().str.strip()\
        .str.replace(r'\. ?', ' ', regex=True)\
        .str.replace(',', '', regex=False)\
        .str.replace(r'(\d+) ', '', regex=True)\
        .str.replace('unsat perform', 'unsatisfactory performance', regex=False)\
        .str.replace('neg of duty', 'neglect of duty', regex=False)\
        .str.replace(r'cond unbecom(ing)?', 'conduct unbecoming', regex=True)\
        .str.replace('unauth force', 'unauthorized force', regex=False)\
        .str.replace('use of dept equip', 'use of department equipment', regex=False)\
        .str.replace('unknown', '', regex=False)
    return df.drop(columns='nature_of_complaint')


def split_rows_with_multiple_charges(df):
    i = 0
    for idx in df[df.charges.str.contains(r'/')].index:
        s = df.loc[idx + i, 'charges']
        parts = re.split(r'\s*(?:/)\s*', s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, 'charges'] = name
        i += len(parts) - 1
    return df


def clean_action(df):
    df.loc[:, 'action'] = df.action_taken.str.lower().str.strip()\
        .str.replace(r'no action ?(tak[eo]n)?', '', regex=True)
    return df.drop(columns=('action_taken'))


def consolidate_action_and_disposition(df):
    df.loc[:, 'action'] = df.action.str.cat(df.disposition, sep='|')\
        .str.replace(r'(not)? ?sustained ?|exonerated ?|unfounded ?|invalid complaint ?', '', regex=True)\
        .str.replace(r'^\|', '', regex=True)\
        .str.replace(r'\|$', '', regex=True)\
        .str.replace(r'(\d+) (\w+)', r'\1-\2', regex=True)
    return df


def clean_disposition(df):
    df.loc[:, 'disposition'] = df.disposition.str.lower().str.strip()\
        .str.replace('invalid complaint', '', regex=False)\
        .str.replace('suspended', '', regex=False)
    return df


def split_rows_with_multiple_officers(df):
    i = 0
    for idx in df[df.name.str.contains(r"/|,")].index:
        s = df.loc[idx + i, "name"]
        parts = re.split(r"\s*(?:/|,)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "name"] = name
        i += len(parts) - 1
    return df


def drop_rows_missing_disp_charges_and_action(df):
    return df[~((df.disposition == '') & (df.charges == '') & (df.action == ''))]


def assign_empty_first_name_column(df):
    df.loc[:, 'first_name'] = ''
    return df


def review_first_names_from_post():
    df = pd.read_csv(data_file_path('match/lake_charles_pd_cprr_2020_extracted_first_names.csv'))
    df = df\
        .pipe(clean_column_names)
    return df


def assign_first_names_from_post(df):
    df.loc[:, 'name'] = df.name.str.lower().str.strip()\
        .str.replace('torres', 'torres paul', regex=False)\
        .str.replace('redd', 'redd jeffrey', regex=False)\
        .str.replace('romero', 'romero mayo', regex=False)\
        .str.replace('nevels', 'nevels harold', regex=False)\
        .str.replace('manuel', 'manuel carlos', regex=False)\
        .str.replace('morrow', 'morrow errel', regex=False)\
        .str.replace('mccloskey', 'mccloskey john', regex=False)\
        .str.replace('myers', 'myers hannah', regex=False)\
        .str.replace('mccue', 'mccue eddie', regex=False)\
        .str.replace('mills', 'mills logan', regex=False)\
        .str.replace('dougay', 'dougay bennon', regex=False)\
        .str.replace('saunier', 'saunier john', regex=False)\
        .str.replace('ewing', 'ewing joshua', regex=False)\
        .str.replace('johnson', 'johnson martin', regex=False)\
        .str.replace('jackson', 'jackson princeton', regex=False)\
        .str.replace('baccigalopi', 'baccigalopi dakota', regex=False)\
        .str.replace('breaux', 'breaux keithen', regex=False)\
        .str.replace('falcon', 'falcon bendy', regex=False)\
        .str.replace('ford', 'ford raymond', regex=False)\
        .str.replace('perkins', 'perkins carlton', regex=False)\
        .str.replace('ponthieaux', 'ponthieaux wilbert', regex=False)\
        .str.replace('markham', 'markham alan', regex=False)
    names = df.name.str.extract(r'(\w+) ?(.+)?')
    df.loc[:, 'last_name'] = names[0]
    df.loc[:, 'first_name'] = names[1].fillna('')
    return df.drop(columns='name')


def assign_agency(df):
    df.loc[:, "agency"] = "Lake Charles PD"
    return df


def clean():
    df = pd.read_csv(data_file_path('raw/lake_charles_pd/lake_charles_pd_cprr_2020.csv'))
    df = df\
        .pipe(clean_column_names)\
        .rename(columns={
            'date_of_investigation': 'investigation_start_date'
        })\
        .pipe(clean_charges)\
        .pipe(split_rows_with_multiple_charges)\
        .pipe(clean_action)\
        .pipe(consolidate_action_and_disposition)\
        .pipe(clean_disposition)\
        .pipe(split_rows_with_multiple_officers)\
        .pipe(drop_rows_missing_disp_charges_and_action)\
        .pipe(assign_empty_first_name_column)\
        .pipe(assign_first_names_from_post)\
        .pipe(assign_agency)\
        .pipe(float_to_int_str, ['investigation_start_date'])\
        .pipe(gen_uid, ['first_name', 'last_name', 'agency'])\
        .pipe(gen_uid, ['first_name', 'last_name', 'investigation_start_date', 'charges', 'action'], 'complaint_uid')
    return df


if __name__ == '__main__':
    df = clean()
    ensure_data_dir('clean')
    df.to_csv(data_file_path('clean/cprr_lake_charles_pd_2020.csv'), index=False)
