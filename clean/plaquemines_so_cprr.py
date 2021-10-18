from lib.columns import clean_column_names
from lib.path import data_file_path, ensure_data_dir
from lib.uid import gen_uid
from lib.clean import clean_names
import pandas as pd
import sys
sys.path.append('../')


def gen_middle_initial(df):
    df.loc[df.middle_name.isna(
    ), 'middle_name'] = df.loc[df.middle_name.isna(), 'middle_initial']
    df.loc[:, 'middle_initial'] = df.middle_name.fillna(
        '').map(lambda x: x[:1])
    return df


def assign_agency(df):
    df.loc[:, 'data_production_year'] = 2019
    df.loc[:, 'agency'] = 'Plaquemines SO'
    return df


def split_name(df):
    df.loc[:, 'against'] = df.against.str.lower().str.strip()\
        .str.replace('smith tam', 'smith/tam', regex=False)\
        .str.replace(r'^robin thomas grant solis gerald cormier jennifer daigle$',
                     'robin thomas/grant solis/gerald cormier/jennifer daigle', regex=True)\
        .str.replace(r'^danny rees brandon leblanc anthony dugas$',
                     'danny rees/brandon leblanc/anthony dugas', regex=True)
    return df


def clean19():
    df = pd.read_csv(data_file_path(
        'raw/plaquemines_so/plaquemines_so_cprr_2019.csv'
    ))
    df = clean_column_names(df)
    df = df.rename(columns={
        'rule_violation': 'charges'
    })
    return df\
        .pipe(gen_middle_initial)\
        .pipe(assign_agency)\
        .pipe(clean_names, ['first_name', 'last_name', 'middle_name', 'middle_initial'])\
        .pipe(gen_uid, ['agency', 'first_name', 'last_name', 'middle_name', 'middle_initial'])\
        .pipe(gen_uid, ['agency', 'tracking_number'], 'complaint_uid')


def clean20():
    df = pd.read_csv(data_file_path('raw/plaquemines_so/plaquemines_so_cprr_2016_2020.csv'))\
        .pipe(clean_column_names)\
        .drop(columns=['complainant'])\
        .pipe(split_name)
    return df


if __name__ == '__main__':
    df = clean19()
    ensure_data_dir('clean')
    df.to_csv(data_file_path('clean/cprr_plaquemines_so_2019.csv'), index=False)
