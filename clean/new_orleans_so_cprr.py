from lib.columns import clean_column_names
from lib.path import data_file_path, ensure_data_dir
from lib.clean import clean_names
import pandas as pd
import sys
sys.path.append("../")


def remove_header_rows(df):
    return df[
        df.date_received.str.strip() != "Date\rReceived"
    ].reset_index(drop=True)


def rename_columns(df):
    df = df.drop(columns=['month', 'quarter', 'numb_er_of_cases'])
    df = df.rename(columns={
        'date_received': 'receive_date',
        'case_number': 'tracking_number',
        'job_title': 'rank_desc',
        'charge_disposition': 'disposition'
    })
    return df


def remove_carriage_return(df, cols):
    for col in cols:
        df.loc[:, col] = df[col].str.replace(r"-\r", r"-")\
            .str.replace(r"\r", r" ")
    return df


def split_name(df):
    names = df.name_of_accused\
        .str.replace(r"^(.+) ([-\w']+(?: Jr\.)?)$", r"\1@@\2").str.split("@@", expand=True)
    df.loc[:, "last_name"] = names[1]
    names = names[0].str.split(" ", expand=True)
    df.loc[:, "first_name"] = names[0]
    df.loc[:, "middle_initial"] = names[1]
    return df


def clean():
    df = pd.read_csv(data_file_path(
        "new_orleans_so/new_orleans_so_cprr_2019_tabula.csv"))
    df = clean_column_names(df)
    return df\
        .pipe(remove_header_rows)\
        .pipe(rename_columns)\
        .pipe(split_name)\
        .pipe(remove_carriage_return, [
            'name_of_accused', 'disposition', 'charges', 'summary', 'assigned_agent',
            'terminated_resigned'
        ])\
        .pipe(clean_names, ['first_name', 'middle_initial', 'last_name'])
