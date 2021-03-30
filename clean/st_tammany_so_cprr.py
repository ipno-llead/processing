from lib.columns import clean_column_names
from lib.path import data_file_path, ensure_data_dir
from lib.clean import split_names, float_to_int_str, clean_names
from lib.uid import gen_uid
import pandas as pd
import sys
sys.path.append('../')


def remove_newlines(df):
    for col in ['full_name', 'raw_occur_date', 'charges']:
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
    df.loc[:, 'occur_year'] = df.raw_occur_date.fillna(
        '').str.replace(r'.+(\d{4})$', r'\1')
    dates = df.raw_occur_date.str.extract(r'^(\d+)\/(\d+)\/\d{4}$')
    df.loc[:, 'occur_month'] = dates[0]
    df.loc[:, 'occur_day'] = dates[1]
    return df


def assign_agency(df):
    df.loc[:, 'agency'] = 'St. Tammany SO'
    df.loc[:, 'data_production_year'] = 2019
    return df


def clean():
    df = pd.read_csv(
        data_file_path(
            'st_tammany_so/st.tammany_so_cprr_lists_2015-2019_tabula.csv')
    )
    df = clean_column_names(df)
    df = df.rename(columns={
        'dept_code': 'department_code',
        'incident_date': 'raw_occur_date',
        'infraction': 'charges'
    })
    df = df\
        .pipe(remove_newlines)\
        .pipe(split_names, "full_name")\
        .pipe(clean_names, ['first_name', 'last_name', 'middle_name'])\
        .pipe(float_to_int_str, ["department_code"])\
        .pipe(pad_dept_code)\
        .pipe(assign_department_desc)\
        .pipe(extract_occur_date)\
        .pipe(assign_agency)\
        .pipe(gen_uid, ['first_name', 'last_name', 'agency'])\
        .pipe(gen_uid, ['agency', 'raw_occur_date', 'uid'], 'complaint_uid')
    df = df.drop(columns=['full_name'])
    return df


if __name__ == '__main__':
    df = clean()
    ensure_data_dir('clean')
    df.to_csv(data_file_path(
        'clean/cprr_st_tammany_so_2015_2019.csv'), index=False)
