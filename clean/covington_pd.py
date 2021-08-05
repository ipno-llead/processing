import sys

import pandas as pd

from lib.columns import clean_column_names, set_values
from lib.path import data_file_path, ensure_data_dir
from lib.uid import gen_uid
from lib import salary
from lib.clean import clean_names, standardize_desc_cols, clean_salaries

sys.path.append('../')


def split_names(df, col, drop=True):
    if col not in df.columns:
        return df
    names = df[col].str.extract(r'^(\w+), (\w+)(?: (\w+))?$')
    df.loc[:, 'first_name'] = names[1]
    df.loc[:, 'middle_name'] = names[2]
    df.loc[:, 'last_name'] = names[0]
    df.loc[:, 'middle_initial'] = df.middle_name.fillna('').map(lambda x: x[:1])
    if drop:
        df = df.drop(columns=[col])
    return df


def assign_agency(df):
    df.loc[:, 'data_production_year'] = 2021
    df.loc[:, 'agency'] = 'Covington PD'
    return df


def clean_rank_desc(df):
    df.rank_desc = df.rank_desc.str.lower().str.strip()\
        .str.replace('police ', '', regex=False)\
        .str.replace(r' i\b', '', regex=True)\
        .str.replace(r'^tac', '', regex=True)\
        .str.replace('&', '', regex=False)\
        .str.replace('  admin suppport spe', ' administrative support specialist', regex=False)\
        .str.replace(r'^ ', '', regex=True)
    return df


def clean_actions_history():
    return pd.read_csv(data_file_path(
        'raw/covington_pd/covington_pd_actions_history.csv'
    )).pipe(clean_column_names)\
        .rename(columns={
            'emp': 'employee_id',
            'job_title': 'rank_desc',
        })\
        .drop(columns=['pay', 'mst_stat'])\
        .drop_duplicates(ignore_index=True)\
        .pipe(split_names, 'name')\
        .pipe(assign_agency)\
        .pipe(clean_names, ['first_name', 'last_name', 'middle_name', 'middle_initial'])\
        .pipe(gen_uid, ['agency', 'employee_id'])\
        .pipe(standardize_desc_cols, ['rank_desc', 'action_desc'])\
        .pipe(clean_rank_desc)


def sum_salaries(df):
    records = []
    record = None
    for _, row in df.iterrows():
        if record is None or row['employee_id'] != record['employee_id']:
            record = row.to_dict()
            if record is not None:
                records.append(record)
        else:
            record['salary'] += row['salary']
    df = pd.DataFrame.from_records(records)
    df.loc[:, 'salary'] = df.salary.map(lambda x: '%.2f' % x)
    return df


def clean_pprr_2010_2020(filename):
    return pd.read_csv(data_file_path(filename)).pipe(clean_column_names)\
        .rename(columns={
            'emp': 'employee_id',
            'year': 'salary_year',
            'employee_gross': 'salary',
        })\
        .drop(columns=['code'])\
        .pipe(set_values, {
            'salary_freq': salary.YEARLY,
            'data_production_year': 2020,
            'agency': 'Covington PD',
        })\
        .pipe(clean_names, ['first_name', 'last_name'])\
        .pipe(clean_salaries, ['salary'])\
        .pipe(sum_salaries)\
        .pipe(gen_uid, ['agency', 'employee_id'])


def clean_pprr_2021():
    # deprecated: this dataset doesn't have employee_id so discarded for now.
    return pd.read_csv(data_file_path(
        'raw/covington_pd/covington_pd_pprr_2021.csv'
    )).pipe(clean_column_names)\
        .rename(columns={
            'description': 'rank_desc',
            'position': 'employee_id',
        })\
        .drop(columns=['stat'])\
        .pipe(split_names, 'employee_name')\
        .pipe(clean_names, ['first_name', 'last_name', 'middle_name', 'middle_initial'])\
        .pipe(standardize_desc_cols, ['rank_desc'])\
        .pipe(set_values, {
            'data_production_year': 2021,
            'agency': 'Covington PD'
        })\
        .pipe(gen_uid, ['agency', 'employee_id'])


def clean_pprr():
    return pd.concat([
        clean_pprr_2010_2020('raw/covington_pd/covington_pd_pprr_2010.csv'),
        clean_pprr_2010_2020('raw/covington_pd/covington_pd_pprr_2011.csv'),
        clean_pprr_2010_2020('raw/covington_pd/covington_pd_pprr_2012.csv'),
        clean_pprr_2010_2020('raw/covington_pd/covington_pd_pprr_2013.csv'),
        clean_pprr_2010_2020('raw/covington_pd/covington_pd_pprr_2014.csv'),
        clean_pprr_2010_2020('raw/covington_pd/covington_pd_pprr_2015.csv'),
        clean_pprr_2010_2020('raw/covington_pd/covington_pd_pprr_2016.csv'),
        clean_pprr_2010_2020('raw/covington_pd/covington_pd_pprr_2017.csv'),
        clean_pprr_2010_2020('raw/covington_pd/covington_pd_pprr_2018.csv'),
        clean_pprr_2010_2020('raw/covington_pd/covington_pd_pprr_2019.csv'),
        clean_pprr_2010_2020('raw/covington_pd/covington_pd_pprr_2020.csv'),
    ])


if __name__ == '__main__':
    actions_history = clean_actions_history()
    pprr = clean_pprr()
    ensure_data_dir('clean')
    actions_history.to_csv(data_file_path(
        'clean/actions_history_covington_pd_2021.csv'
    ), index=False)
    pprr.to_csv(data_file_path(
        'clean/pprr_covington_pd_2020.csv'
    ), index=False)
