from lib.columns import clean_column_names
from lib.path import data_file_path, ensure_data_dir
from lib.clean import split_names, float_to_int_str
import pandas as pd
import sys
sys.path.append('../')


def remove_newlines(df):
    for col in ['full_name', 'incident_date', 'infraction']:
        df.loc[:, col] = df[col].str.replace(r'(\d)\r\n(\d)', r'\1\2')\
            .str.replace(r'\r\n', ' ')
    return df


def pad_dept_code(df):
    df.loc[df.department_code != "",
           'department_code'] = df.department_code.str.zfill(2)
    return df


def clean_depts():
    df = pd.read_csv(data_file_path(
        "st_tammany_so/st_tammany_department_codes_tabula.csv"
    ))
    df = clean_column_names(df)
    df = df.rename(columns={'loc': 'department_code',
                            'org_code': 'department_desc'})
    # df.loc[:, 'department_code'] = df.department_code.str.replace(r'\*$', '')
    return df


def clean():
    df = pd.read_csv(
        data_file_path(
            'st_tammany_so/st.tammany_so_cprr_lists_2015-2019_tabula.csv')
    )
    df = clean_column_names(df)
    df = df.rename(columns={
        'dept_code': 'department_code'
    })
    return df\
        .pipe(remove_newlines)\
        .pipe(split_names, "full_name")\
        .pipe(float_to_int_str, ["department_code"])\
        .pipe(pad_dept_code)
