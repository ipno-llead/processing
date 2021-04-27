from lib.columns import clean_column_names
from lib.path import data_file_path, ensure_data_dir
from lib.clean import split_names, float_to_int_str, clean_names
from lib.uid import gen_uid
import pandas as pd
import sys
sys.path.append('../')


def remove_newlines(df):
    for col in ['full_name', 'occur_raw_date', 'charges']:
        df.loc[:, col] = df[col].str.replace(r'(\d)\r\n(\d)', r'\1\2')\
            .str.replace(r'\r\n', ' ')
    return df


def pad_dept_code(df):
    df.loc[df.department_code != "",
           'department_code'] = df.department_code.str.zfill(2)
    return df


def assign_department_desc(df):
    dept_df = pd.read_csv(data_file_path(
        "st_tammany_so/st_tammany_department_codes_tabula.csv"
    ))
    dept_df = clean_column_names(dept_df)
    dept_df.loc[:, 'loc'] = dept_df.loc[:, 'loc'].str.replace(r'\*$', '')
    dept_df = dept_df.set_index('loc')
    dept_dict = dept_df.org_code.to_dict()
    dept_dict["20"] = "St. Tammany Parish Jail"

    df.loc[:, "department_desc"] = df.department_code.map(
        lambda x: dept_dict.get(x, ''))
    return df


def extract_occur_date(df):
    df.loc[:, 'occur_year'] = df.occur_raw_date.fillna(
        '').str.replace(r'.+(\d{4})$', r'\1')
    dates = df.occur_raw_date.str.extract(r'^(\d+)\/(\d+)\/\d{4}$')
    df.loc[:, 'occur_month'] = dates[0]
    df.loc[:, 'occur_day'] = dates[1]
    return df


def assign_agency(df):
    df.loc[:, 'agency'] = 'St. Tammany SO'
    df.loc[:, 'data_production_year'] = 2021
    return df


def gen_middle_initial(df):
    df.loc[:, 'middle_initial'] = df.middle_name.fillna(
        '').map(lambda x: x[:1])
    return df


def remove_new_lines_from_charges(df):
    df.loc[:, 'charges'] = df.charges.str.replace(r'(\n|\r)\s*', ' ')
    return df


def clean():
    df = pd.concat([
        pd.read_csv(data_file_path(
            'st_tammany_so/st_tammany_so_cprr_2011-2020_tabula.csv')),
        pd.read_csv(data_file_path(
            'st_tammany_so/st_tammany_so_cprr_2020-2021_tabula.csv'))
    ])
    df = clean_column_names(df)
    df = df.rename(columns={
        'dept': 'department_code',
        'date_of_incident': 'occur_raw_date',
        'discipline_action_outcome': 'charges'
    })
    df = df\
        .pipe(split_names, "name")\
        .pipe(float_to_int_str, ["department_code"])\
        .pipe(pad_dept_code)\
        .pipe(assign_department_desc)\
        .pipe(extract_occur_date)\
        .pipe(assign_agency)\
        .pipe(clean_names, ['first_name', 'last_name', 'middle_name'])\
        .pipe(gen_middle_initial)\
        .pipe(gen_uid, ['first_name', 'last_name', 'agency'])\
        .pipe(gen_uid, ['agency', 'occur_year', 'occur_month', 'occur_day', 'uid', 'charges'], 'charge_uid')\
        .pipe(gen_uid, ['charge_uid'], 'complaint_uid')\
        .pipe(remove_new_lines_from_charges)
    df = df.drop(columns=['name'])
    return df


if __name__ == '__main__':
    df = clean()
    ensure_data_dir('clean')
    df.to_csv(data_file_path(
        'clean/cprr_st_tammany_so_2011_2021.csv'), index=False)
