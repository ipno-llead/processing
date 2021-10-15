import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path
from lib.columns import clean_column_names
from lib.uid import gen_uid


def split_name(df):
    col_name = [col for col in df.columns if col.endswith('name')][0]
    names = df[col_name].str.strip().str.lower()\
        .str.replace('van tran', 'vantran', regex=False).str.replace("de' clouet", "de'clouet", regex=False)\
        .str.replace(r'(\w+) \b(\w{2})$', r'\2 \1', regex=True).str.replace(r"\"", "", regex=True)\
        .str.extract(r'^(\w+) ?(\w{3,})? ?(jr|sr)? (\w+-?\'?\w+)$')

    df.loc[:, 'first_name'] = names[0]
    df.loc[:, 'middle_name'] = names[1]\
        .fillna('')
    df.loc[:, 'suffix'] = names[2]\
        .fillna('')
    df.loc[:, 'last_name'] = names[3]
    df.loc[:, 'last_name'] = df.last_name.str.cat(df.suffix, sep=' ')
    return df.drop(columns=['name', 'suffix'])


def clean_charges(df):
    df.loc[:, 'charges'] = df.reason\
        .str.replace(r'^(\w+)-(\w+)', r'\1 \2', regex=True)\
        .str.replace(r'\/', '|', regex=True)\
        .str.replace('-', ': ', regex=False)
    return df.drop(columns='reason')


def assign_action(df):
    df.loc[:, 'action'] = 'decertified'
    return df


def rename_agency(df):
    df.loc[:, 'agency'] = df.agency\
        .str.replace(r'(\w+) SO', r'\1 Parish SO', regex=True)\
        .str.replace(r'(Orleans PD|NOPD)', 'New Orleans PD', regex=True)\
        .str.replace('Harbor PD', 'New Orleans Harbor PD', regex=False)\
        .str.replace('LSP', 'LA State Police', regex=False)\
        .str.replace('EBRSO', 'E. Baton Rouge Parish SO', regex=False)
    return df


def clean():
    df = pd.read_csv(data_file_path('raw/post_council/post_decertifications_2016_2019.csv'))\
        .pipe(clean_column_names)\
        .rename(columns={
            'date': 'decertification_date'
        })\
        .pipe(split_name)\
        .pipe(clean_charges)\
        .pipe(assign_action)\
        .pipe(rename_agency)\
        .pipe(gen_uid, ['first_name', 'last_name', 'agency'])\
        .pipe(gen_uid, ['uid', 'charges', 'decertification_date'], 'complaint_uid')
    return df


if __name__ == '__main__':
    df = clean()
    df.to_csv(data_file_path(
        'clean/cprr_post_2016_2019.csv'), index=False)
